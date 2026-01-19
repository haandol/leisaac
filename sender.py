#!/usr/bin/env python3
"""
Mac sender - reads SO-ARM 101 leader directly via Feetech SDK
Publishes joint positions over ZMQ PUB (msgpack)

Run:
  python3 sender.py \
    --port /dev/tty.usbmodem5AE60578541 \
    --connect tcp://<EC2_IP>:5555 \
    --hz 100
"""

import argparse
import json
import os
import time
import msgpack
import zmq

# Feetech STS3215 constants
PROTOCOL_END = 0
BAUDRATE = 1000000
ADDR_PRESENT_POSITION = 56  # STS3215 present position address
LEN_PRESENT_POSITION = 2
RESOLUTION = 4096  # STS3215 encoder resolution

MOTOR_NAMES = ["shoulder_pan", "shoulder_lift", "elbow_flex", "wrist_flex", "wrist_roll", "gripper"]
# Note: Physical motor IDs - swap 3 and 4 if elbow_flex and wrist_flex are swapped on hardware
MOTOR_IDS = [1, 2, 4, 3, 5, 6]

# Normalization modes matching LeIsaac
NORM_MODE_RANGE_M100_100 = "RANGE_M100_100"  # -100 to 100 degrees
NORM_MODE_RANGE_0_100 = "RANGE_0_100"  # 0 to 100 degrees

MOTOR_NORM_MODES = {
    "shoulder_pan": NORM_MODE_RANGE_M100_100,
    "shoulder_lift": NORM_MODE_RANGE_M100_100,
    "elbow_flex": NORM_MODE_RANGE_M100_100,
    "wrist_flex": NORM_MODE_RANGE_M100_100,
    "wrist_roll": NORM_MODE_RANGE_M100_100,
    "gripper": NORM_MODE_RANGE_0_100,
}


def load_calibration(calibration_path: str) -> dict:
    """Load calibration from JSON file."""
    with open(calibration_path) as f:
        return json.load(f)


def raw_to_degree(raw_value: int, motor_name: str, calibration: dict) -> float:
    """Convert raw encoder value to degrees using calibration.
    
    Matches LeIsaac's motors_bus.py _normalize() logic exactly.
    """
    cal = calibration[motor_name]
    range_min = cal["range_min"]
    range_max = cal["range_max"]

    if range_max == range_min:
        return 0.0

    # Bound value to calibration range (same as LeIsaac)
    bounded_val = min(range_max, max(range_min, raw_value))
    
    norm_mode = MOTOR_NORM_MODES[motor_name]
    if norm_mode == NORM_MODE_RANGE_M100_100:
        # Map to -100 to 100
        norm = (((bounded_val - range_min) / (range_max - range_min)) * 200) - 100
        return norm
    else:  # RANGE_0_100
        # Map to 0 to 100
        norm = ((bounded_val - range_min) / (range_max - range_min)) * 100
        return norm


def create_bus(port: str):
    """Create Feetech motor bus connection."""
    try:
        from scservo_sdk import PortHandler, PacketHandler, GroupSyncRead
    except ImportError:
        raise ImportError("Install scservo_sdk: pip install scservo-sdk")

    port_handler = PortHandler(port)
    packet_handler = PacketHandler(PROTOCOL_END)

    if not port_handler.openPort():
        raise RuntimeError(f"Failed to open port {port}")
    if not port_handler.setBaudRate(BAUDRATE):
        raise RuntimeError(f"Failed to set baudrate {BAUDRATE}")

    # Setup sync read for all motors
    sync_read = GroupSyncRead(port_handler, packet_handler, ADDR_PRESENT_POSITION, LEN_PRESENT_POSITION)
    for mid in MOTOR_IDS:
        sync_read.addParam(mid)

    return port_handler, packet_handler, sync_read


def read_positions(sync_read, packet_handler, calibration: dict | None) -> dict[str, float]:
    """Read all motor positions via sync read."""
    result = sync_read.txRxPacket()
    if result != 0:
        print(f"[WARN] Sync read error: {packet_handler.getTxRxResult(result)}")
        return {}

    positions = {}
    for name, mid in zip(MOTOR_NAMES, MOTOR_IDS):
        if sync_read.isAvailable(mid, ADDR_PRESENT_POSITION, LEN_PRESENT_POSITION):
            raw = sync_read.getData(mid, ADDR_PRESENT_POSITION, LEN_PRESENT_POSITION)
            if calibration:
                positions[f"{name}.pos"] = raw_to_degree(raw, name, calibration)
            else:
                positions[f"{name}.pos"] = float(raw)
        else:
            positions[f"{name}.pos"] = 0.0
    return positions


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", required=True, help="Mac serial port, e.g. /dev/tty.usbmodemXXXX")
    ap.add_argument("--connect", required=True, help="EC2 endpoint, e.g. tcp://<EC2_IP>:5555")
    ap.add_argument("--topic", default="so101", help="PUB topic")
    ap.add_argument("--hz", type=float, default=100.0, help="Publish rate")
    ap.add_argument("--print_every", type=int, default=50, help="Print every N messages")
    ap.add_argument("--calibration", default=None, help="Path to calibration JSON file (so101_leader.json)")
    args = ap.parse_args()

    # Load calibration if provided
    calibration = None
    if args.calibration:
        if os.path.exists(args.calibration):
            calibration = load_calibration(args.calibration)
            print(f"[MAC] Loaded calibration from {args.calibration}")
        else:
            print(f"[WARN] Calibration file not found: {args.calibration}, using raw values")
    else:
        print("[WARN] No calibration file provided (--calibration), using raw encoder values")

    print(f"[MAC] Opening port {args.port}...")
    port_handler, packet_handler, sync_read = create_bus(args.port)
    print(f"[MAC] Port opened, baudrate={BAUDRATE}")

    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.PUB)
    sock.setsockopt(zmq.SNDHWM, 1)
    sock.connect(args.connect)
    print(f"[MAC] ZMQ PUB connect to {args.connect} topic={args.topic} hz={args.hz}")
    time.sleep(0.8)  # PUB/SUB handshake

    period = 1.0 / max(args.hz, 1.0)
    seq = 0

    try:
        while True:
            t0 = time.time()
            raw_action = read_positions(sync_read, packet_handler, calibration)

            payload = {"seq": seq, "ts": t0, "raw_action": raw_action}
            packed = msgpack.packb(payload, use_bin_type=True)
            sock.send_multipart([args.topic.encode("utf-8"), packed])

            if seq % args.print_every == 0:
                vals = [f"{v:.1f}" for v in raw_action.values()]
                print(f"[MAC] seq={seq} pos=[{', '.join(vals)}]")

            seq += 1
            dt = time.time() - t0
            if (sleep := period - dt) > 0:
                time.sleep(sleep)

    except KeyboardInterrupt:
        print("\n[MAC] Stopping...")
    finally:
        port_handler.closePort()
        sock.close(0)
        ctx.term()
        print("[MAC] Clean exit.")


if __name__ == "__main__":
    main()
