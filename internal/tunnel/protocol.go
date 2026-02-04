package tunnel

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
)

// Handshake obfuscation to reduce DPI fingerprint.
// Format: [padding_len:1][padding:N][xor_data:M]
// XOR key is derived from a fixed seed to avoid transmitting key.

var obfuscateKey = []byte{0x7a, 0x3f, 0x9c, 0x2e, 0x5d, 0x8b, 0x1a, 0x4f, 0x6c, 0x0e, 0x9d, 0x3b, 0x7f, 0x2c, 0x5a, 0x8e}

// ObfuscateHandshake obfuscates handshake data with XOR and random padding.
func ObfuscateHandshake(data []byte) []byte {
	// Generate random padding (8-64 bytes)
	paddingLen := 8 + randomByte()%57
	padding := make([]byte, paddingLen)
	rand.Read(padding)

	// XOR the data
	xorData := make([]byte, len(data))
	for i := range data {
		xorData[i] = data[i] ^ obfuscateKey[i%len(obfuscateKey)]
	}

	// Format: [padding_len:1][padding:N][xor_data:M]
	result := make([]byte, 1+int(paddingLen)+len(xorData))
	result[0] = paddingLen
	copy(result[1:1+int(paddingLen)], padding)
	copy(result[1+int(paddingLen):], xorData)

	return result
}

// DeobfuscateHandshake reverses the obfuscation.
func DeobfuscateHandshake(data []byte) ([]byte, error) {
	if len(data) < 2 {
		return nil, errors.New("obfuscated data too short")
	}

	paddingLen := int(data[0])
	if len(data) < 1+paddingLen {
		return nil, errors.New("invalid padding length")
	}

	xorData := data[1+paddingLen:]
	result := make([]byte, len(xorData))
	for i := range xorData {
		result[i] = xorData[i] ^ obfuscateKey[i%len(obfuscateKey)]
	}

	return result, nil
}

func randomByte() byte {
	b := make([]byte, 1)
	rand.Read(b)
	return b[0]
}

// MessageType represents the type of tunnel message.
type MessageType uint8

const (
	// MsgConnect requests a new connection to the target.
	MsgConnect MessageType = 1
	// MsgData carries data for an existing connection.
	MsgData MessageType = 2
	// MsgClose closes a connection.
	MsgClose MessageType = 3
	// MsgPing is a heartbeat message.
	MsgPing MessageType = 4
	// MsgPong is a heartbeat response.
	MsgPong MessageType = 5
	// MsgKeyExchange exchanges nonce for session key derivation.
	MsgKeyExchange MessageType = 6
)

const (
	// KeyExchangeNonceSize is the size of nonce for key exchange (32 bytes).
	KeyExchangeNonceSize = 32
)

// Message represents a message in the tunnel protocol.
// Wire format: [type:1][connID:8][length:4][payload:length]
type Message struct {
	Type    MessageType
	ConnID  uint64
	Payload []byte
}

const (
	// HeaderSize is the size of the message header.
	HeaderSize = 1 + 8 + 4 // type + connID + length
	// MaxPayloadSize is the maximum payload size.
	MaxPayloadSize = 64 * 1024 // 64KB
)

// ConnID protocol flags - use bit 63 to distinguish TCP/UDP
// This is backward compatible: existing TCP connections have bit 63 = 0
const (
	// ConnIDProtocolUDP marks a connection ID as UDP (bit 63 set)
	ConnIDProtocolUDP uint64 = 1 << 63
	// ConnIDValueMask extracts the actual connection ID value
	ConnIDValueMask uint64 = ^ConnIDProtocolUDP
)

// MakeUDPConnID creates a UDP-flagged connection ID
func MakeUDPConnID(id uint64) uint64 {
	return id | ConnIDProtocolUDP
}

// IsUDPConnID checks if connID represents a UDP connection
func IsUDPConnID(connID uint64) bool {
	return connID&ConnIDProtocolUDP != 0
}

// GetConnIDValue extracts the actual connection ID value (without protocol flag)
func GetConnIDValue(connID uint64) uint64 {
	return connID & ConnIDValueMask
}

var (
	// ErrPayloadTooLarge is returned when payload exceeds MaxPayloadSize.
	ErrPayloadTooLarge = errors.New("payload too large")
	// ErrInvalidMessage is returned when message format is invalid.
	ErrInvalidMessage = errors.New("invalid message format")
)

// Encode encodes the message to bytes.
func (m *Message) Encode() ([]byte, error) {
	if len(m.Payload) > MaxPayloadSize {
		return nil, ErrPayloadTooLarge
	}

	buf := make([]byte, HeaderSize+len(m.Payload))
	buf[0] = byte(m.Type)
	binary.BigEndian.PutUint64(buf[1:9], m.ConnID)
	binary.BigEndian.PutUint32(buf[9:13], uint32(len(m.Payload)))
	copy(buf[13:], m.Payload)

	return buf, nil
}

// DecodeMessage decodes a message from reader.
func DecodeMessage(r io.Reader) (*Message, error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	msgType := MessageType(header[0])
	connID := binary.BigEndian.Uint64(header[1:9])
	length := binary.BigEndian.Uint32(header[9:13])

	if length > MaxPayloadSize {
		return nil, ErrPayloadTooLarge
	}

	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
	}

	return &Message{
		Type:    msgType,
		ConnID:  connID,
		Payload: payload,
	}, nil
}

// NewConnectMessage creates a connect message.
func NewConnectMessage(connID uint64) *Message {
	return &Message{
		Type:   MsgConnect,
		ConnID: connID,
	}
}

// NewDataMessage creates a data message.
func NewDataMessage(connID uint64, data []byte) *Message {
	return &Message{
		Type:    MsgData,
		ConnID:  connID,
		Payload: data,
	}
}

// NewCloseMessage creates a close message.
func NewCloseMessage(connID uint64) *Message {
	return &Message{
		Type:   MsgClose,
		ConnID: connID,
	}
}

// NewPingMessage creates a ping message.
func NewPingMessage() *Message {
	return &Message{
		Type: MsgPing,
	}
}

// NewPongMessage creates a pong message.
func NewPongMessage() *Message {
	return &Message{
		Type: MsgPong,
	}
}

// NewKeyExchangeMessage creates a key exchange message with nonce.
func NewKeyExchangeMessage(nonce []byte) *Message {
	return &Message{
		Type:    MsgKeyExchange,
		Payload: nonce,
	}
}

// NewUDPConnectMessage creates a UDP connect message with client address payload.
func NewUDPConnectMessage(connID uint64, clientAddr string) *Message {
	return &Message{
		Type:    MsgConnect,
		ConnID:  MakeUDPConnID(connID),
		Payload: []byte(clientAddr),
	}
}

// NewUDPDataMessage creates a UDP data message.
func NewUDPDataMessage(connID uint64, data []byte) *Message {
	return &Message{
		Type:    MsgData,
		ConnID:  MakeUDPConnID(connID),
		Payload: data,
	}
}

// NewUDPCloseMessage creates a UDP close message.
func NewUDPCloseMessage(connID uint64) *Message {
	return &Message{
		Type:   MsgClose,
		ConnID: MakeUDPConnID(connID),
	}
}
