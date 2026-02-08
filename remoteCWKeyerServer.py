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

# Try to import serial library for COM port DTR control
try:
    import serial
    SERIAL_AVAILABLE = True
except ImportError:
    SERIAL_AVAILABLE = False
    print("Warning: pyserial not available. COM port keying will be simulated.")
    print("Install with: pip install pyserial")

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


class SerialKeyer:
    """
    Serial port DTR/RTS-based CW keying interface
    
    Controls CW keying via DTR (Data Terminal Ready) and/or RTS (Request To Send)
    pins on a COM/serial port. This is a common method for keying radios,
    especially with USB-to-Serial adapters.
    
    Typical usage:
    - DTR pin: CW Key (carrier on/off)
    - RTS pin: PTT (push-to-talk)
    
    Or for simple interfaces:
    - DTR: Key
    - RTS: not used (set to None)
    """
    
    def __init__(self, port: str, key_signal: str = 'dtr', ptt_signal: Optional[str] = 'rts',
                 invert_key: bool = False, invert_ptt: bool = False):
        """
        Initialize serial port keyer
        
        Args:
            port: Serial port name (e.g., 'COM3' on Windows, '/dev/ttyUSB0' on Linux)
            key_signal: Which signal to use for key ('dtr' or 'rts')
            ptt_signal: Which signal to use for PTT ('dtr', 'rts', or None to disable)
            invert_key: Invert key signal (False=high for key down, True=low for key down)
            invert_ptt: Invert PTT signal
        """
        self.port = port
        self.key_signal = key_signal.lower()
        self.ptt_signal = ptt_signal.lower() if ptt_signal else None
        self.invert_key = invert_key
        self.invert_ptt = invert_ptt
        self.serial_port = None
        self.serial_initialized = False
        self.key_state = False
        self.ptt_state = False
        
        if not SERIAL_AVAILABLE:
            print(f"Serial keyer: pyserial not available - running in SIMULATION mode")
            return
        
        try:
            # Open serial port
            # We don't need to send/receive data, just control DTR/RTS
            self.serial_port = serial.Serial(
                port=port,
                baudrate=9600,  # Doesn't matter for DTR/RTS control
                timeout=0
            )
            
            # Initialize signals to OFF state
            self._set_signal(self.key_signal, False)
            if self.ptt_signal:
                self._set_signal(self.ptt_signal, False)
            
            self.serial_initialized = True
            ptt_info = f", PTT={self.ptt_signal.upper()}" if self.ptt_signal else ""
            print(f"Serial keyer initialized: {port} (KEY={self.key_signal.upper()}{ptt_info})")
            
        except Exception as e:
            print(f"Failed to initialize serial port {port}: {e}")
            self.serial_initialized = False
    
    def _set_signal(self, signal: str, state: bool):
        """
        Set DTR or RTS signal state
        
        Args:
            signal: 'dtr' or 'rts'
            state: True for high, False for low
        """
        if not self.serial_port:
            return
        
        try:
            if signal == 'dtr':
                self.serial_port.dtr = state
            elif signal == 'rts':
                self.serial_port.rts = state
            
            # CRITICAL: Add small delay to prevent USB adapter crash
            # Some cheap USB-Serial adapters (especially PL2303) can't handle
            # rapid DTR/RTS changes and will hang with error -32 (EPIPE)
            time.sleep(0.002)  # 2ms delay between operations
            
        except Exception as e:
            print(f"Error setting {signal.upper()}: {e}")
    
    def set_key_state(self, key_down: bool):
        """
        Set the CW key state
        
        Args:
            key_down: True for key down (carrier ON), False for key up
        """
        self.key_state = key_down
        
        # Apply inversion if needed
        signal_state = (not key_down) if self.invert_key else key_down
        
        if self.serial_initialized:
            self._set_signal(self.key_signal, signal_state)
        
        state_str = "DOWN (TX ON)" if key_down else "UP (TX OFF)"
        signal_str = "HIGH" if signal_state else "LOW"
        print(f"[SERIAL] Key {state_str} ({self.key_signal.upper()}={signal_str})")
    
    def set_ptt_state(self, ptt_on: bool):
        """
        Set the PTT (Push-To-Talk) state
        
        Args:
            ptt_on: True for PTT on, False for PTT off
        """
        if not self.ptt_signal:
            return
        
        self.ptt_state = ptt_on
        
        # Apply inversion if needed
        signal_state = (not ptt_on) if self.invert_ptt else ptt_on
        
        if self.serial_initialized:
            self._set_signal(self.ptt_signal, signal_state)
        
        state_str = "ON" if ptt_on else "OFF"
        signal_str = "HIGH" if signal_state else "LOW"
        print(f"[SERIAL] PTT {state_str} ({self.ptt_signal.upper()}={signal_str})")
    
    def cleanup(self):
        """Cleanup serial port resources"""
        if self.serial_port:
            try:
                # Set both signals low before closing
                self._set_signal(self.key_signal, False)
                if self.ptt_signal:
                    self._set_signal(self.ptt_signal, False)
                
                self.serial_port.close()
                print(f"Serial port {self.port} closed")
            except:
                pass


class CwKeyerThread:
    """
    Thread that plays back CW keying patterns from the FIFO
    
    This implements the intelligent latency control and timing from
    the original KeyerThread() in the C implementation.
    
    Works with both GPIOKeyer and SerialKeyer.
    """
    
    def __init__(self, keyer):
        """
        Initialize keyer thread
        
        Args:
            keyer: Either GPIOKeyer or SerialKeyer instance
        """
        self.keyer = keyer
        self.rx_fifo = CwKeyingFifo()
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.current_key_state = False
        self.next_transition_time_ms = 0
        self.pending_state = False
        self.state_change_pending = False
        
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
        """
        Main keyer loop - runs in separate thread
        
        CW Stream format - RELATIVE timing:
        Each byte contains:
        - Bit 7: Key state (1=down, 0=up)  
        - Bits 6-0: Time in ms SINCE PREVIOUS EVENT
        """
        
        while self.running:
            try:
                current_time_ms = int(time.time() * 1000)
                
                # Check if we have a pending state change to apply
                if self.state_change_pending:
                    if current_time_ms >= self.next_transition_time_ms:
                        # Apply the state change
                        self.current_key_state = self.pending_state
                        self.keyer.set_key_state(self.pending_state)
                        self.state_change_pending = False
                        # Don't process next event yet - wait one loop iteration
                
                # If no pending change, try to get next event from FIFO
                elif not self.state_change_pending:
                    elem = self.rx_fifo.pop()
                    
                    if elem:
                        new_state, delay_ms = CwStreamEncoder.decode_key_event(elem.cmd)
                        
                        # Schedule when to apply this state
                        self.next_transition_time_ms = current_time_ms + delay_ms
                        self.pending_state = new_state
                        self.state_change_pending = True
                        
                    else:
                        # FIFO empty - ensure key is up
                        if self.current_key_state:
                            self.current_key_state = False
                            self.keyer.set_key_state(False)
                
                # Sleep for 1ms
                time.sleep(0.001)
                
            except Exception as e:
                print(f"Error in keyer loop: {e}")
                import traceback
                traceback.print_exc()
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
    """Enhanced CW Network Server with Morse decoding and GPIO/Serial support"""
    
    def __init__(self, host: str = '0.0.0.0', port: int = 7355,
                 # GPIO options
                 gpio_key_pin: int = 17, gpio_ptt_pin: Optional[int] = 27,
                 enable_gpio: bool = False,
                 # Serial port options
                 serial_port: Optional[str] = None,
                 serial_key_signal: str = 'dtr',
                 serial_ptt_signal: Optional[str] = 'rts',
                 serial_invert_key: bool = False,
                 serial_invert_ptt: bool = False):
        """
        Initialize CW Network Server
        
        Args:
            host: Bind address
            port: Listen port
            
            GPIO mode options:
            gpio_key_pin: GPIO pin for KEY (default 17)
            gpio_ptt_pin: GPIO pin for PTT (default 27)
            enable_gpio: Enable GPIO mode (default False)
            
            Serial port mode options:
            serial_port: COM port name (e.g., 'COM3' or '/dev/ttyUSB0')
            serial_key_signal: Signal for KEY ('dtr' or 'rts', default 'dtr')
            serial_ptt_signal: Signal for PTT ('dtr', 'rts', or None, default 'rts')
            serial_invert_key: Invert KEY signal polarity
            serial_invert_ptt: Invert PTT signal polarity
            
        Note: GPIO and Serial port are mutually exclusive.
              If both are specified, Serial port takes priority.
        """
        self.host = host
        self.port = port
        self.server_socket: Optional[socket.socket] = None
        self.clients: List[CwNetClient] = []
        self.running = False
        self.accept_thread: Optional[threading.Thread] = None
        self.poll_thread: Optional[threading.Thread] = None
        
        # Server configuration - everyone has full permissions
        self.accept_users = {
            "Max": 7,
            "Moritz": 7,
            "Guest": 7
        }
        
        # Radio parameters (simulated)
        self.vfo_frequency = 14.050e6
        self.operation_mode = 1  # CW mode
        self.s_meter = -120
        
        # Keyer initialization - Serial port takes priority over GPIO
        self.keyer = None
        self.keyer_thread = None
        
        if serial_port:
            # Serial port mode
            print(f"Initializing Serial keyer on {serial_port}...")
            self.keyer = SerialKeyer(
                port=serial_port,
                key_signal=serial_key_signal,
                ptt_signal=serial_ptt_signal,
                invert_key=serial_invert_key,
                invert_ptt=serial_invert_ptt
            )
            if self.keyer.serial_initialized:
                self.keyer_thread = CwKeyerThread(self.keyer)
        elif enable_gpio:
            # GPIO mode
            print(f"Initializing GPIO keyer...")
            self.keyer = GPIOKeyer(gpio_key_pin, gpio_ptt_pin)
            if self.keyer.gpio_initialized:
                self.keyer_thread = CwKeyerThread(self.keyer)
        else:
            print("Running in SIMULATION mode (no hardware keying)")
        
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
        
        # Cleanup keyer (GPIO or Serial)
        if self.keyer:
            self.keyer.cleanup()
        
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
        
        # Decode and log the Morse events
        for byte_val in payload:
            key_down, wait_ms = CwStreamEncoder.decode_key_event(byte_val)
            state_str = "DOWN" if key_down else "UP"
            print(f"[MORSE]   Key {state_str} after {wait_ms}ms")
        
        # Send to keyer
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
        if current_time - client.last_vfo_report_time > 0.5:
            vfo_report = VfoReport(self.vfo_frequency, 0, self.s_meter)
            self._send_command(client, CwNetCmd.FREQ_REPORT, vfo_report.pack())
            client.last_vfo_report_time = current_time
    
    def _disconnect_client(self, client: CwNetClient):
        """Disconnect a client"""
        print(f"Disconnecting client {client.address} (rx={client.bytes_received}, tx={client.bytes_sent})")
        client.close()
        if client in self.clients:
            self.clients.remove(client)


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='CW Network Server with GPIO/Serial Keying')
    parser.add_argument('--host', default='0.0.0.0', help='Bind address (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=7355, help='Port number (default: 7355)')
    
    # Keying mode selection
    keying_mode = parser.add_mutually_exclusive_group()
    keying_mode.add_argument('--gpio', action='store_true', help='Use GPIO keying (Raspberry Pi)')
    keying_mode.add_argument('--serial', type=str, metavar='PORT', help='Use serial port keying (e.g., COM3, /dev/ttyUSB0)')
    
    # GPIO options
    gpio_group = parser.add_argument_group('GPIO options (when --gpio is used)')
    gpio_group.add_argument('--key-pin', type=int, default=17, help='GPIO pin for CW key (default: 17)')
    gpio_group.add_argument('--ptt-pin', type=int, default=27, help='GPIO pin for PTT (default: 27)')
    
    # Serial port options
    serial_group = parser.add_argument_group('Serial port options (when --serial is used)')
    serial_group.add_argument('--key-signal', choices=['dtr', 'rts'], default='dtr',
                              help='Signal for KEY (default: dtr)')
    serial_group.add_argument('--ptt-signal', choices=['dtr', 'rts', 'none'], default='rts',
                              help='Signal for PTT (default: rts, use "none" to disable)')
    serial_group.add_argument('--invert-key', action='store_true',
                              help='Invert KEY signal (low=key down)')
    serial_group.add_argument('--invert-ptt', action='store_true',
                              help='Invert PTT signal (low=ptt on)')
    
    args = parser.parse_args()
    
    # Determine keying mode
    enable_gpio = args.gpio
    serial_port = args.serial
    serial_ptt_signal = None if args.ptt_signal == 'none' else args.ptt_signal
    
    # Create server
    server = CwNetServer(
        host=args.host,
        port=args.port,
        # GPIO options
        gpio_key_pin=args.key_pin,
        gpio_ptt_pin=args.ptt_pin,
        enable_gpio=enable_gpio,
        # Serial port options
        serial_port=serial_port,
        serial_key_signal=args.key_signal,
        serial_ptt_signal=serial_ptt_signal,
        serial_invert_key=args.invert_key,
        serial_invert_ptt=args.invert_ptt
    )
    
    if server.start():
        print("Server running. Press Ctrl+C to stop.")
        
        # Show keying mode
        if serial_port:
            print(f"Keying mode: SERIAL PORT ({serial_port})")
            print(f"  KEY signal: {args.key_signal.upper()}{' (inverted)' if args.invert_key else ''}")
            if serial_ptt_signal:
                print(f"  PTT signal: {serial_ptt_signal.upper()}{' (inverted)' if args.invert_ptt else ''}")
        elif enable_gpio:
            print(f"Keying mode: GPIO (KEY={args.key_pin}, PTT={args.ptt_pin})")
        else:
            print("Keying mode: SIMULATION (no hardware)")
        
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