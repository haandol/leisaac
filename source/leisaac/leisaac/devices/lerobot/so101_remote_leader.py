"""
SO101 Remote Leader - receives joint positions from Mac via ZMQ
Use this as teleop device on EC2 with LeIsaac
"""

import threading
import time

import msgpack
import zmq

from leisaac.assets.robots.lerobot import SO101_FOLLOWER_MOTOR_LIMITS

from ..device_base import Device


class SO101RemoteLeader(Device):
    """A remote SO101 Leader device that receives data via ZMQ."""

    def __init__(
        self,
        env,
        bind_addr: str = "tcp://*:5555",
        topic: str = "so101",
        timeout_ms: int = 1000,
    ):
        super().__init__(env, "so101_leader")  # same device_type as local leader
        self.bind_addr = bind_addr
        self.topic = topic
        self.timeout_ms = timeout_ms

        self._motor_limits = SO101_FOLLOWER_MOTOR_LIMITS
        self._latest_state = {name: 0.0 for name in self._motor_limits}
        self._last_recv_time = 0.0
        self._connected = False
        self._lock = threading.Lock()

        self._start_receiver()

    def __str__(self) -> str:
        return (
            f"SO101-RemoteLeader (ZMQ SUB on {self.bind_addr})\n"
            f"\tWaiting for Mac sender to connect...\n"
        )

    def _add_device_control_description(self):
        self._display_controls_table.add_row(["so101-remote", "receive joint positions from remote Mac via ZMQ"])
        self._display_controls_table.add_row([
            "[TIPS]",
            f"Run sender.py on Mac with --connect tcp://<EC2_IP>:5555",
        ])

    def _start_receiver(self):
        """Start background thread to receive ZMQ messages."""
        self._ctx = zmq.Context.instance()
        self._sock = self._ctx.socket(zmq.SUB)
        self._sock.setsockopt(zmq.RCVHWM, 10)
        self._sock.setsockopt(zmq.RCVTIMEO, 100)  # 100ms timeout instead of CONFLATE
        self._sock.bind(self.bind_addr)
        self._sock.subscribe(self.topic.encode("utf-8"))

        self._running = True
        self._thread = threading.Thread(target=self._recv_loop, daemon=True)
        self._thread.start()
        self._connected = True
        print(f"[EC2] ZMQ SUB bound to {self.bind_addr}, topic={self.topic}")

    def _recv_loop(self):
        """Background loop to receive and parse messages."""
        recv_count = 0
        while self._running:
            try:
                topic, packed = self._sock.recv_multipart()
                payload = msgpack.unpackb(packed, raw=False)
                raw_action = payload.get("raw_action", {})

                with self._lock:
                    for key, val in raw_action.items():
                        # key format: "shoulder_pan.pos" -> "shoulder_pan"
                        motor_name = key.replace(".pos", "")
                        if motor_name in self._latest_state:
                            self._latest_state[motor_name] = val
                    self._last_recv_time = time.time()

                recv_count += 1
                if recv_count == 1:
                    print(f"[EC2] First message received! raw_action keys: {list(raw_action.keys())}")
                if recv_count % 100 == 0:
                    print(f"[EC2] Received {recv_count} messages, latest state: {self._latest_state}")
            except zmq.Again:
                # Timeout, no message available
                pass
            except Exception as e:
                print(f"[EC2] ZMQ recv error: {e}")

    def get_device_state(self):
        """Return latest joint positions (same format as SO101Leader)."""
        with self._lock:
            return dict(self._latest_state)

    def input2action(self):
        ac_dict = super().input2action()
        ac_dict["motor_limits"] = self._motor_limits
        return ac_dict

    @property
    def motor_limits(self) -> dict[str, tuple[float, float]]:
        return self._motor_limits

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def has_recent_data(self) -> bool:
        """Check if we received data recently."""
        return (time.time() - self._last_recv_time) < (self.timeout_ms / 1000.0)

    def disconnect(self):
        self._running = False
        if self._thread.is_alive():
            self._thread.join(timeout=1.0)
        self._sock.close()
        self._connected = False
        print("[EC2] SO101-RemoteLeader disconnected.")

    def connect(self):
        if not self._connected:
            self._start_receiver()

    def configure(self):
        pass

    def calibrate(self):
        print("[EC2] Remote leader uses Mac-side calibration. No local calibration needed.")
