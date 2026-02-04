#!/usr/bin/env python3
"""
CW Network Server Emulator - Enhanced Version
With Morse code encoding/decoding and GPIO keying support

Based on the C implementation from CwNet.c/CwNet.h and CwStreamEnc.c/CwStreamEnc.h
Author: Claude (based on code by Wolfgang Buescher DL4YHF)
Date: 2026-02-04
"""

import socket
import struct
import threading
import time
import select
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple
from enum import IntEnum
from collections import deque

# Try to import GPIO library (will fail on non-Raspberry Pi systems)
try:
    import RPi.GPIO as GPIO
    GPIO_AVAILABLE = True
except ImportError:
    GPIO_AVAILABLE = False
    print("Warning: RPi.GPIO not available. GPIO keying will be simulated.")

# Protocol Constants
CWNET_MAX_CLIENTS = 3
CW_KEYING_FIFO_SIZE = 128

# Command codes (from CwNet.h)
class CwNetCmd(IntEnum):
    NONE = 0x00
    CONNECT = 0x01
    DISCONN = 0x02
    PING = 0x03
    PRINT = 0x04
    TX_INFO = 0x05
    RIGCTLD = 0x06
    MORSE = 0x10
    AUDIO = 0x11
    VORBIS = 0x12
    RSV13 = 0x13
    CI_V = 0x14
    SPECTRUM = 0x15
    FREQ_REPORT = 0x16
    PARAM_INTEGER = 0x18
    PARAM_DOUBLE = 0x19
    PARAM_STRING = 0x1A
    QUERY_PARAM = 0x1B
    MULTI_FUNCTION_METER_REPORT = 0x20
    POTI_REPORT = 0x21
    BAND_STACKING_REGISTER = 0x22
    USER_DEFINED_BAND = 0x23
    SERIAL_PORT_TUNNEL_ANY = 0x30
    SERIAL_PORT_TUNNEL_1 = 0x31
    SERIAL_PORT_TUNNEL_2 = 0x32
    SERIAL_PORT_TUNNEL_3 = 0x33
    RESERVE = 0xFF

# Block length masks
CWNET_CMD_MASK_BLOCKLEN = 0xC0
CWNET_CMD_MASK_NO_BLOCK = 0x00
CWNET_CMD_MASK_SHORT_BLOCK = 0x40
CWNET_CMD_MASK_LONG_BLOCK = 0x80
CWNET_CMD_MASK_RESERVE = 0xC0
CWNET_CMD_MASK_COMMAND = 0x3F

# Payload sizes
CWNET_PAYLOAD_SIZE_PING = 16
CWNET_STREAM_SAMPLING_RATE = 8000
CWNET_CMD_DATA_BUFFER_SIZE = 2048

# Client states
class ClientState(IntEnum):
    IDLE = 0
    ACCEPTED = 1
    LOGIN_SENT = 2
    LOGIN_CFMD = 3
    DISCONN = 4

# Magic number for sanity checks
CWNET_MAGIC_PI = 0x31415926


class CwStreamEncoder:
    """
    CW Stream Encoder/Decoder based on CwStreamEnc.c
    
    Encoding format:
    - Bit 7: Key state (1=down/carrier ON, 0=up/carrier OFF)
    - Bits 6-0: 7-bit timestamp (time to wait BEFORE applying the key state)
    
    Time encoding (non-linear for efficient compression):
    - 0-31 ms: Direct encoding (1 ms resolution)
    - 32-156 ms: 0x20 + (t-32)/4 (4 ms resolution)
    - 157-1165 ms: 0x40 + (t-157)/16 (16 ms resolution)
    """
    
    @staticmethod
    def milliseconds_to_7bit_timestamp(milliseconds: int) -> int:
        """Convert milliseconds to 7-bit timestamp"""
        if milliseconds < 0:
            return 0x00
        if milliseconds <= 31:
            return milliseconds
        if milliseconds <= 156:
            return 0x20 + (milliseconds - 32) // 4
        if milliseconds <= 1165:
            return 0x40 + (milliseconds - 157) // 16
        return 0x7F  # Maximum encodable time
    
    @staticmethod
    def timestamp_7bit_to_milliseconds(timestamp: int) -> int:
        """Convert 7-bit timestamp to milliseconds"""
        timestamp &= 0x7F  # Strip key up/down bit
        
        if timestamp <= 0x1F:  # 0-31
            return timestamp
        if timestamp <= 0x3F:  # 32-156
            return 32 + 4 * (timestamp - 0x20)
        # 157-1165
        return 157 + 16 * (timestamp - 0x40)
    
    @staticmethod
    def encode_key_event(key_down: bool, milliseconds_to_wait: int) -> List[int]:
        """
        Encode a key up/down event into one or more bytes
        
        Args:
            key_down: True for key down (carrier ON), False for key up
            milliseconds_to_wait: Time since previous event
            
        Returns:
            List of encoded bytes
        """
        encoded_bytes = []
        remaining_ms = milliseconds_to_wait
        
        while True:
            # Encode up to 1165ms per byte
            t_ms = min(remaining_ms, 1165)
            timestamp = CwStreamEncoder.milliseconds_to_7bit_timestamp(t_ms)
            
            byte_val = timestamp
            if key_down:
                byte_val |= 0x80  # Set MSB for key down
            
            encoded_bytes.append(byte_val)
            
            remaining_ms -= t_ms
            if remaining_ms <= 0:
                break
        
        return encoded_bytes
    
    @staticmethod
    def decode_key_event(byte_val: int) -> Tuple[bool, int]:
        """
        Decode a key up/down event byte
        
        Args:
            byte_val: Encoded byte
            
        Returns:
            Tuple of (key_down, milliseconds_to_wait)
        """
        key_down = (byte_val & 0x80) != 0
        milliseconds = CwStreamEncoder.timestamp_7bit_to_milliseconds(byte_val & 0x7F)
        return key_down, milliseconds


@dataclass
class CwKeyingFifoElement:
    """Single element in the CW keying FIFO"""
    cmd: int  # Command byte (key state + timestamp)
    reception_time_ms: int  # Time of reception


class CwKeyingFifo:
    """FIFO for CW keying events"""
    
    def __init__(self):
        self.elements: List[Optional[CwKeyingFifoElement]] = [None] * CW_KEYING_FIFO_SIZE
        self.head_index = 0
        self.tail_index = 0
    
    def push(self, cmd: int, reception_time_ms: int = 0):
        """Add element to FIFO"""
        self.elements[self.head_index] = CwKeyingFifoElement(cmd, reception_time_ms)
        self.head_index = (self.head_index + 1) % CW_KEYING_FIFO_SIZE
    
    def pop(self) -> Optional[CwKeyingFifoElement]:
        """Remove and return element from FIFO"""
        if self.is_empty():
            return None
        elem = self.elements[self.tail_index]
        self.tail_index = (self.tail_index + 1) % CW_KEYING_FIFO_SIZE
        return elem
    
    def peek(self) -> Optional[CwKeyingFifoElement]:
        """Look at next element without removing it"""
        if self.is_empty():
            return None
        return self.elements[self.tail_index]
    
    def is_empty(self) -> bool:
        """Check if FIFO is empty"""
        return self.head_index == self.tail_index
    
    def get_buffered_milliseconds(self) -> int:
        """Get total milliseconds of keying patterns in FIFO"""
        total_ms = 0
        index = self.tail_index
        
        while index != self.head_index:
            elem = self.elements[index]
            if elem:
                _, ms = CwStreamEncoder.decode_key_event(elem.cmd)
                total_ms += ms
            index = (index + 1) % CW_KEYING_FIFO_SIZE
        
        return total_ms
    
    def check_for_end_of_transmission(self) -> bool:
        """
        Check if FIFO contains end-of-transmission marker
        (two consecutive key-up commands)
        """
        index = self.tail_index
        prev_byte = None
        first_entry = True
        
        while index != self.head_index:
            elem = self.elements[index]
            if elem:
                if not first_entry and prev_byte is not None:
                    # Two consecutive key-up (bit 7 clear) = EOT
                    if (elem.cmd & 0x80) == 0 and (prev_byte & 0x80) == 0:
                        return True
                prev_byte = elem.cmd
                first_entry = False
            index = (index + 1) % CW_KEYING_FIFO_SIZE
        
        return False


class GPIOKeyer:
    """
    GPIO-based CW keying interface for Raspberry Pi
    
    Handles the physical keying of the radio transmitter via GPIO pins.
    """
    
    def __init__(self, key_pin: int = 17, ptt_pin: Optional[int] = 27, 
                 sidetone_enabled: bool = True):
        """
        Initialize GPIO keyer
        
        Args:
            key_pin: GPIO pin for CW key output (default: BCM 17)
            ptt_pin: GPIO pin for PTT output (default: BCM 27, None to disable)
            sidetone_enabled: Enable audio sidetone generation
        """
        self.key_pin = key_pin
        self.ptt_pin = ptt_pin
        self.sidetone_enabled = sidetone_enabled
        self.gpio_initialized = False
        self.key_state = False  # False = key up, True = key down
        self.ptt_state = False
        
        if GPIO_AVAILABLE:
            try:
                GPIO.setmode(GPIO.BCM)
                GPIO.setup(self.key_pin, GPIO.OUT)
                GPIO.output(self.key_pin, GPIO.LOW)  # Start with key up
                
                if self.ptt_pin is not None:
                    GPIO.setup(self.ptt_pin, GPIO.OUT)
                    GPIO.output(self.ptt_pin, GPIO.LOW)  # Start with PTT off
                
                self.gpio_initialized = True
                print(f"GPIO keyer initialized: KEY=GPIO{self.key_pin}, PTT=GPIO{self.ptt_pin}")
            except Exception as e:
                print(f"Failed to initialize GPIO: {e}")
                self.gpio_initialized = False
        else:
            print("GPIO keyer running in SIMULATION mode (no hardware)")
    
    def set_key_state(self, key_down: bool):
        """
        Set the CW key state
        
        Args:
            key_down: True for key down (carrier ON), False for key up
        """
        self.key_state = key_down
        
        if self.gpio_initialized:
            GPIO.output(self.key_pin, GPIO.HIGH if key_down else GPIO.LOW)
        
        # Log state changes
        state_str = "DOWN (TX ON)" if key_down else "UP (TX OFF)"
        print(f"[GPIO] Key {state_str}")
        
        # TODO: Generate sidetone if enabled
        if self.sidetone_enabled:
            self._generate_sidetone(key_down)
    
    def set_ptt_state(self, ptt_on: bool):
        """
        Set the PTT (Push-To-Talk) state
        
        Args:
            ptt_on: True for PTT on, False for PTT off
        """
        if self.ptt_pin is None:
            return
        
        self.ptt_state = ptt_on
        
        if self.gpio_initialized:
            GPIO.output(self.ptt_pin, GPIO.HIGH if ptt_on else GPIO.LOW)
        
        state_str = "ON" if ptt_on else "OFF"
        print(f"[GPIO] PTT {state_str}")
    
    def _generate_sidetone(self, key_down: bool):
        """
        Generate audio sidetone (placeholder)
        
        In a real implementation, this would generate a tone using:
        - PyAudio
        - ALSA direct access
        - Hardware PWM on another GPIO pin
        """
        # TODO: Implement actual sidetone generation
        pass
    
    def cleanup(self):
        """Cleanup GPIO resources"""
        if self.gpio_initialized:
            try:
                GPIO.cleanup([self.key_pin])
                if self.ptt_pin is not None:
                    GPIO.cleanup([self.ptt_pin])
                print("GPIO cleaned up")
            except:
                pass


class CwKeyerThread:
    """
    Thread that plays back CW keying patterns from the FIFO
    
    This implements the intelligent latency control and timing from
    the original KeyerThread() in the C implementation.
    """
    
    def __init__(self, gpio_keyer: GPIOKeyer):
        self.gpio_keyer = gpio_keyer
        self.rx_fifo = CwKeyingFifo()
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.current_key_state = False
        self.next_transition_time_ms = 0
        
    def start(self):
        """Start the keyer thread"""
        self.running = True
        self.thread = threading.Thread(target=self._keyer_loop, daemon=True)
        self.thread.start()
        print("CW Keyer thread started")
    
    def stop(self):
        """Stop the keyer thread"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=1.0)
        print("CW Keyer thread stopped")
    
    def add_morse_data(self, morse_bytes: bytes):
        """
        Add received Morse data to the playback FIFO
        
        Args:
            morse_bytes: Raw bytes from CWNET_CMD_MORSE packet
        """
        current_time_ms = int(time.time() * 1000)
        
        for byte_val in morse_bytes:
            self.rx_fifo.push(byte_val, current_time_ms)
    
    def _keyer_loop(self):
        """Main keyer loop - runs in separate thread"""
        while self.running:
            try:
                current_time_ms = int(time.time() * 1000)
                
                # Check if we need to transition to a new state
                if current_time_ms >= self.next_transition_time_ms:
                    # Get next event from FIFO
                    elem = self.rx_fifo.pop()
                    
                    if elem:
                        key_down, wait_ms = CwStreamEncoder.decode_key_event(elem.cmd)
                        
                        # Apply the new key state
                        self.current_key_state = key_down
                        self.gpio_keyer.set_key_state(key_down)
                        
                        # Schedule next transition
                        self.next_transition_time_ms = current_time_ms + wait_ms
                    else:
                        # FIFO empty - ensure key is up
                        if self.current_key_state:
                            self.current_key_state = False
                            self.gpio_keyer.set_key_state(False)
                
                # Sleep for 1ms (high precision timing)
                time.sleep(0.001)
                
            except Exception as e:
                print(f"Error in keyer loop: {e}")
                time.sleep(0.01)


@dataclass
class ConnectData:
    """Structure for connection data (T_CwNet_ConnectData)"""
    user_name: str = ""
    callsign: str = ""
    permissions: int = 0

    def pack(self) -> bytes:
        """Pack the connect data into binary format"""
        user_bytes = self.user_name.encode('utf-8')[:43].ljust(44, b'\x00')
        call_bytes = self.callsign.encode('utf-8')[:43].ljust(44, b'\x00')
        perm_bytes = struct.pack('<I', self.permissions)
        return user_bytes + call_bytes + perm_bytes

    @staticmethod
    def unpack(data: bytes):
        """Unpack binary data into ConnectData"""
        if len(data) < 92:
            raise ValueError("Invalid ConnectData size")
        user_name = data[:44].rstrip(b'\x00').decode('utf-8', errors='ignore')
        callsign = data[44:88].rstrip(b'\x00').decode('utf-8', errors='ignore')
        permissions = struct.unpack('<I', data[88:92])[0]
        return ConnectData(user_name, callsign, permissions)


@dataclass
class VfoReport:
    """Structure for VFO report"""
    vfo_frequency: float = 14.050e6
    tx_status: int = 0
    s_meter_level_db: int = -120
    
    def pack(self) -> bytes:
        """Pack VFO report into binary format"""
        return struct.pack('<dBb', self.vfo_frequency, self.tx_status, self.s_meter_level_db)


class CwNetClient:
    """Represents a connected client"""
    
    def __init__(self, sock: socket.socket, addr: tuple):
        self.socket = sock
        self.address = addr
        self.ip = addr[0]
        self.port = addr[1]
        self.state = ClientState.ACCEPTED
        self.rx_buffer = bytearray()
        self.tx_buffer = bytearray()
        self.user_name = ""
        self.callsign = ""
        self.permissions = 0
        self.last_activity = time.time()
        self.bytes_received = 0
        self.bytes_sent = 0
        self.ping_timestamp = [0, 0, 0]
        self.last_ping_time = 0
        self.vfo_report_hash = 0
        self.last_vfo_report_time = 0
        self.is_transmitting = False  # Is this client currently keying?
        
    def send_data(self, data: bytes):
        """Send data to client"""
        try:
            self.socket.sendall(data)
            self.bytes_sent += len(data)
            self.last_activity = time.time()
            return True
        except Exception as e:
            print(f"Error sending to {self.address}: {e}")
            return False
    
    def close(self):
        """Close the client connection"""
        try:
            self.socket.close()
        except:
            pass


class CwNetServer:
    """Enhanced CW Network Server with Morse decoding and GPIO support"""
    
    def __init__(self, host: str = '0.0.0.0', port: int = 7355,
                 gpio_key_pin: int = 17, gpio_ptt_pin: Optional[int] = 27,
                 enable_gpio: bool = True):
        self.host = host
        self.port = port
        self.server_socket: Optional[socket.socket] = None
        self.clients: List[CwNetClient] = []
        self.running = False
        self.accept_thread: Optional[threading.Thread] = None
        self.poll_thread: Optional[threading.Thread] = None
        
        # Server configuration
        self.accept_users = {
            "Max": 7,
            "Moritz": 7,
            "Guest": 1
        }
        
        # Radio parameters (simulated)
        self.vfo_frequency = 14.050e6
        self.operation_mode = 1  # CW mode
        self.s_meter = -120
        
        # GPIO keyer
        self.enable_gpio = enable_gpio
        if enable_gpio:
            self.gpio_keyer = GPIOKeyer(gpio_key_pin, gpio_ptt_pin)
            self.keyer_thread = CwKeyerThread(self.gpio_keyer)
        else:
            self.gpio_keyer = None
            self.keyer_thread = None
        
        # Current transmitting client (-1 = none)
        self.transmitting_client_index = -1
    
    def start(self):
        """Start the server"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(CWNET_MAX_CLIENTS)
            self.running = True
            
            print(f"CW Network Server started on {self.host}:{self.port}")
            
            # Start keyer thread
            if self.keyer_thread:
                self.keyer_thread.start()
            
            # Start accept thread
            self.accept_thread = threading.Thread(target=self._accept_connections, daemon=True)
            self.accept_thread.start()
            
            # Start polling thread
            self.poll_thread = threading.Thread(target=self._poll_clients, daemon=True)
            self.poll_thread.start()
            
            return True
        except Exception as e:
            print(f"Failed to start server: {e}")
            return False
    
    def stop(self):
        """Stop the server"""
        self.running = False
        
        # Stop keyer thread
        if self.keyer_thread:
            self.keyer_thread.stop()
        
        # Cleanup GPIO
        if self.gpio_keyer:
            self.gpio_keyer.cleanup()
        
        # Close all client connections
        for client in self.clients:
            client.close()
        self.clients.clear()
        
        # Close server socket
        if self.server_socket:
            self.server_socket.close()
            self.server_socket = None
        
        print("CW Network Server stopped")
    
    def _accept_connections(self):
        """Accept incoming client connections"""
        while self.running:
            try:
                if self.server_socket:
                    self.server_socket.settimeout(1.0)
                    try:
                        sock, addr = self.server_socket.accept()
                        if len(self.clients) < CWNET_MAX_CLIENTS:
                            client = CwNetClient(sock, addr)
                            self.clients.append(client)
                            print(f"Client connected from {addr[0]}:{addr[1]}")
                        else:
                            print(f"Connection rejected from {addr} - max clients reached")
                            sock.close()
                    except socket.timeout:
                        continue
            except Exception as e:
                if self.running:
                    print(f"Error accepting connection: {e}")
    
    def _poll_clients(self):
        """Poll clients for data and send periodic updates"""
        while self.running:
            try:
                current_time = time.time()
                clients_to_remove = []
                
                for client in self.clients:
                    # Check for timeout (30 seconds)
                    if current_time - client.last_activity > 30:
                        print(f"Client {client.address} timed out")
                        clients_to_remove.append(client)
                        continue
                    
                    # Check for incoming data
                    ready = select.select([client.socket], [], [], 0.01)
                    if ready[0]:
                        try:
                            data = client.socket.recv(4096)
                            if data:
                                client.rx_buffer.extend(data)
                                client.bytes_received += len(data)
                                client.last_activity = current_time
                                self._process_rx_buffer(client)
                            else:
                                clients_to_remove.append(client)
                        except Exception as e:
                            print(f"Error receiving from {client.address}: {e}")
                            clients_to_remove.append(client)
                    
                    # Send periodic updates for logged-in clients
                    if client.state == ClientState.LOGIN_CFMD:
                        self._send_periodic_updates(client, current_time)
                
                # Remove disconnected clients
                for client in clients_to_remove:
                    self._disconnect_client(client)
                
                time.sleep(0.02)  # 20ms polling interval
                
            except Exception as e:
                if self.running:
                    print(f"Error in poll loop: {e}")
    
    def _process_rx_buffer(self, client: CwNetClient):
        """Process received data from client"""
        while len(client.rx_buffer) > 0:
            if len(client.rx_buffer) < 1:
                break
            
            cmd_byte = client.rx_buffer[0]
            cmd = cmd_byte & CWNET_CMD_MASK_COMMAND
            block_type = cmd_byte & CWNET_CMD_MASK_BLOCKLEN
            
            header_size = 1
            payload_size = 0
            
            if block_type == CWNET_CMD_MASK_NO_BLOCK:
                if cmd == CwNetCmd.PING:
                    payload_size = CWNET_PAYLOAD_SIZE_PING
                elif cmd == CwNetCmd.CONNECT:
                    payload_size = 92
            elif block_type == CWNET_CMD_MASK_SHORT_BLOCK:
                if len(client.rx_buffer) < 2:
                    break
                payload_size = client.rx_buffer[1]
                header_size = 2
            elif block_type == CWNET_CMD_MASK_LONG_BLOCK:
                if len(client.rx_buffer) < 3:
                    break
                payload_size = struct.unpack('<H', bytes(client.rx_buffer[1:3]))[0]
                header_size = 3
            
            total_size = header_size + payload_size
            if len(client.rx_buffer) < total_size:
                break
            
            payload = bytes(client.rx_buffer[header_size:total_size])
            self._execute_command(client, cmd, payload)
            client.rx_buffer = client.rx_buffer[total_size:]
    
    def _execute_command(self, client: CwNetClient, cmd: int, payload: bytes):
        """Execute received command"""
        try:
            if cmd == CwNetCmd.CONNECT:
                self._handle_connect(client, payload)
            elif cmd == CwNetCmd.DISCONN:
                self._handle_disconnect(client)
            elif cmd == CwNetCmd.PING:
                self._handle_ping(client, payload)
            elif cmd == CwNetCmd.MORSE:
                self._handle_morse(client, payload)
            elif cmd == CwNetCmd.RIGCTLD:
                self._handle_rigctld(client, payload)
            elif cmd == CwNetCmd.PRINT:
                self._handle_print(client, payload)
            else:
                print(f"Unknown command from {client.address}: 0x{cmd:02X}")
        except Exception as e:
            print(f"Error executing command 0x{cmd:02X} from {client.address}: {e}")
    
    def _handle_connect(self, client: CwNetClient, payload: bytes):
        """Handle connection request"""
        try:
            conn_data = ConnectData.unpack(payload)
            print(f"Connection request from {client.address}: user='{conn_data.user_name}', call='{conn_data.callsign}'")
            
            if conn_data.user_name in self.accept_users:
                permissions = self.accept_users[conn_data.user_name]
                client.user_name = conn_data.user_name
                client.callsign = conn_data.callsign
                client.permissions = permissions
                client.state = ClientState.LOGIN_CFMD
                
                response = ConnectData(conn_data.user_name, conn_data.callsign, permissions)
                self._send_command(client, CwNetCmd.CONNECT, response.pack())
                
                welcome_msg = f"Welcome {conn_data.user_name} ({conn_data.callsign})! Connected to CW Network Server."
                self._send_command(client, CwNetCmd.PRINT, welcome_msg.encode('utf-8'))
                
                print(f"User '{conn_data.user_name}' logged in with permissions: {permissions}")
            else:
                print(f"User '{conn_data.user_name}' rejected")
                self._send_command(client, CwNetCmd.DISCONN, b'')
        except Exception as e:
            print(f"Error handling connect: {e}")
    
    def _handle_disconnect(self, client: CwNetClient):
        """Handle disconnect request"""
        print(f"Client {client.address} requested disconnect")
        client.state = ClientState.DISCONN
    
    def _handle_ping(self, client: CwNetClient, payload: bytes):
        """Handle ping request"""
        if len(payload) >= 16:
            req_resp = payload[0]
            identifier = payload[1]
            t1 = struct.unpack('<I', payload[4:8])[0]
            t2 = struct.unpack('<I', payload[8:12])[0]
            t3 = struct.unpack('<I', payload[12:16])[0]
            
            if req_resp == 0:
                current_time_ms = int(time.time() * 1000) & 0x7FFFFFFF
                response = struct.pack('<BBH', 1, identifier, 0)
                response += struct.pack('<III', t1, current_time_ms, 0)
                self._send_command(client, CwNetCmd.PING, response)
            elif req_resp == 1:
                latency_ms = (t2 - t1) if t2 > t1 else 0
                print(f"Ping response from {client.address}: latency ~{latency_ms}ms")
    
    def _handle_morse(self, client: CwNetClient, payload: bytes):
        """
        Handle Morse code data
        
        This is where the received CW keying patterns are processed
        and sent to the GPIO keyer for actual transmission.
        """
        print(f"[MORSE] Received {len(payload)} bytes from {client.address} ({client.user_name})")
        
        # Check if this client has permission to transmit
        if not (client.permissions & 0x02):  # Bit 1 = TX permission
            print(f"[MORSE] Client {client.user_name} doesn't have TX permission")
            return
        
        # Decode and log the Morse events
        for byte_val in payload:
            key_down, wait_ms = CwStreamEncoder.decode_key_event(byte_val)
            state_str = "DOWN" if key_down else "UP"
            print(f"[MORSE]   Key {state_str} after {wait_ms}ms")
        
        # Send to GPIO keyer
        if self.keyer_thread:
            self.keyer_thread.add_morse_data(payload)
            client.is_transmitting = True
        
        # Broadcast to other clients
        for other_client in self.clients:
            if other_client != client and other_client.state == ClientState.LOGIN_CFMD:
                self._send_command(other_client, CwNetCmd.MORSE, payload)
        
        # Send TX_INFO to notify who is transmitting
        tx_info = f"{client.user_name} ({client.callsign})".encode('utf-8')
        for other_client in self.clients:
            if other_client.state == ClientState.LOGIN_CFMD:
                self._send_command(other_client, CwNetCmd.TX_INFO, tx_info)
    
    def _handle_rigctld(self, client: CwNetClient, payload: bytes):
        """Handle rigctld command"""
        try:
            cmd_str = payload.decode('utf-8', errors='ignore').strip()
            print(f"[RIGCTLD] Command from {client.address}: {cmd_str}")
            
            if cmd_str.startswith('f') or cmd_str.startswith('get_freq'):
                response = f"{int(self.vfo_frequency)}\n"
                self._send_command(client, CwNetCmd.RIGCTLD, response.encode('utf-8'))
            elif cmd_str.startswith('F') or cmd_str.startswith('set_freq'):
                parts = cmd_str.split()
                if len(parts) > 1:
                    try:
                        self.vfo_frequency = float(parts[1])
                        print(f"[RIGCTLD] Frequency set to {self.vfo_frequency} Hz")
                    except ValueError:
                        pass
        except Exception as e:
            print(f"Error handling rigctld command: {e}")
    
    def _handle_print(self, client: CwNetClient, payload: bytes):
        """Handle print message"""
        try:
            msg = payload.decode('utf-8', errors='ignore')
            print(f"[CHAT] {client.user_name}: {msg}")
            
            # Broadcast to all other clients
            for other_client in self.clients:
                if other_client != client and other_client.state == ClientState.LOGIN_CFMD:
                    broadcast_msg = f"{client.user_name}: {msg}".encode('utf-8')
                    self._send_command(other_client, CwNetCmd.PRINT, broadcast_msg)
        except Exception as e:
            print(f"Error handling print: {e}")
    
    def _send_command(self, client: CwNetClient, cmd: int, payload: bytes):
        """Send command to client"""
        try:
            payload_len = len(payload)
            
            if payload_len == 0:
                packet = bytes([cmd | CWNET_CMD_MASK_NO_BLOCK])
            elif payload_len <= 255:
                packet = bytes([cmd | CWNET_CMD_MASK_SHORT_BLOCK, payload_len]) + payload
            else:
                packet = bytes([cmd | CWNET_CMD_MASK_LONG_BLOCK]) + struct.pack('<H', payload_len) + payload
            
            client.send_data(packet)
        except Exception as e:
            print(f"Error sending command 0x{cmd:02X} to {client.address}: {e}")
    
    def _send_periodic_updates(self, client: CwNetClient, current_time: float):
        """Send periodic updates to logged-in client"""
        # Send ping every 2 seconds
        if current_time - client.last_ping_time > 2.0:
            current_time_ms = int(current_time * 1000) & 0x7FFFFFFF
            ping_payload = struct.pack('<BBH', 0, 0, 0)
            ping_payload += struct.pack('<III', current_time_ms, 0, 0)
            self._send_command(client, CwNetCmd.PING, ping_payload)
            client.last_ping_time = current_time
        
        # Send VFO report every 0.5 seconds
        # if current_time - client.last_vfo_report_time > 0.5:
        #     vfo_report = VfoReport(self.vfo_frequency, 0, self.s_meter)
        #     self._send_command(client, CwNetCmd.FREQ_REPORT, vfo_report.pack())
        #     print(vfo_report.pack())
        #     client.last_vfo_report_time = current_time
    
    def _disconnect_client(self, client: CwNetClient):
        """Disconnect a client"""
        print(f"Disconnecting client {client.address} (rx={client.bytes_received}, tx={client.bytes_sent})")
        client.close()
        if client in self.clients:
            self.clients.remove(client)


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='CW Network Server with GPIO Keying')
    parser.add_argument('--host', default='0.0.0.0', help='Bind address (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=7355, help='Port number (default: 7355)')
    parser.add_argument('--key-pin', type=int, default=17, help='GPIO pin for CW key (default: 17)')
    parser.add_argument('--ptt-pin', type=int, default=27, help='GPIO pin for PTT (default: 27)')
    parser.add_argument('--no-gpio', action='store_true', help='Disable GPIO (simulation mode)')
    
    args = parser.parse_args()
    
    server = CwNetServer(
        host=args.host,
        port=args.port,
        gpio_key_pin=args.key_pin,
        gpio_ptt_pin=args.ptt_pin,
        enable_gpio=not args.no_gpio
    )
    
    if server.start():
        print("Server running. Press Ctrl+C to stop.")
        print(f"GPIO mode: {'ENABLED' if not args.no_gpio else 'SIMULATION'}")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down...")
            server.stop()
    else:
        print("Failed to start server")


if __name__ == '__main__':
    main()