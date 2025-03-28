package helper

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"testing"

	"github.com/involk-secure-1609/goGFS/common"
)

// TestEncodeMessage tests the encoding functionality for various message types
func TestEncodeMessage(t *testing.T) {
	tests := []struct {
		name        string
		messageType common.MessageType
		message     any
	}{
		{
			name:        "ClientMasterReadRequest",
			messageType: common.ClientMasterReadRequestType,
			message: common.ClientMasterReadRequest{
				Filename:   "test.txt",
				ChunkIndex: 5,
			},
		},
		{
			name:        "ClientMasterWriteResponse",
			messageType: common.ClientMasterWriteResponseType,
			message: common.ClientMasterWriteResponse{
				ChunkHandle:           123456789,
				MutationId:            987654321,
				PrimaryChunkServer:    "server1:8080",
				SecondaryChunkServers: []string{"server2:8080", "server3:8080"},
				ErrorMessage:          "",
			},
		},
		{
			name:        "MasterChunkServerHandshakeRequest",
			messageType: common.MasterChunkServerHandshakeRequestType,
			message: common.MasterChunkServerHandshakeRequest{
				ChunkHandles: []int64{1, 2, 3, 4, 5},
			},
		},
		{
			name:  "InterChunkServerCommitRequest",
			messageType: common.InterChunkServerCommitRequestType,
			message: common.InterChunkServerCommitRequest{
				ChunkHandle:   42,
				ChunkOffset:   100,
				MutationOrder: []int64{1, 2, 3},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeMessage(tt.messageType, tt.message)
			if err != nil {
				t.Fatalf("EncodeMessage() error = %v", err)
			}

			// Basic validation of encoded message structure
			if len(encoded) < 3 {
				t.Fatalf("Encoded message too short: %v", encoded)
			}

			// Check message type
			if common.MessageType(encoded[0]) != tt.messageType {
				t.Errorf("Message type mismatch, got %v, want %v", encoded[0], tt.messageType)
			}

			// Check message length
			messageLength := int(encoded[1]) | int(encoded[2])<<8 // little endian
			if messageLength != len(encoded)-3 {
				t.Errorf("Message length mismatch, header says %v bytes, actual payload is %v bytes", 
					messageLength, len(encoded)-3)
			}
		})
	}
}

// TestDecodeMessage tests the decoding functionality for various message types
func TestDecodeMessage(t *testing.T) {
	// Create test messages
	testMessages := []any{
		common.ClientMasterReadRequest{
			Filename:   "test.txt",
			ChunkIndex: 5,
		},
		common.ClientMasterWriteResponse{
			ChunkHandle:           123456789,
			MutationId:            987654321,
			PrimaryChunkServer:    "server1:8080",
			SecondaryChunkServers: []string{"server2:8080", "server3:8080"},
			ErrorMessage:          "",
		},
		common.PrimaryChunkCommitRequest{
			ChunkHandle:      12345,
			MutationId:       67890,
			SecondaryServers: []string{"secondary1:8080", "secondary2:8080"},
		},
		common.ClientChunkServerReadResponse{
			ResponseLength: 1024,
			ErrorMessage:   "no error",
		},
	}

	for _, message := range testMessages {
		t.Run(reflect.TypeOf(message).Name(), func(t *testing.T) {
			// Encode the message directly using gob (bypassing EncodeMessage to isolate test)
			var buf bytes.Buffer
			encoder := gob.NewEncoder(&buf)
			err := encoder.Encode(message)
			if err != nil {
				t.Fatalf("Failed to encode message for testing: %v", err)
			}
			
			encodedBytes := buf.Bytes()

			// Use DecodeMessage to decode it back
			switch message.(type) {
			case common.ClientMasterReadRequest:
				decoded, err := DecodeMessage[common.ClientMasterReadRequest](encodedBytes)
				if err != nil {
					t.Fatalf("DecodeMessage() error = %v", err)
				}
				original := message.(common.ClientMasterReadRequest)
				if decoded.Filename != original.Filename || decoded.ChunkIndex != original.ChunkIndex {
					t.Errorf("Decoded message doesn't match original. Got %+v, want %+v", *decoded, original)
				}
			
			case common.ClientMasterWriteResponse:
				decoded, err := DecodeMessage[common.ClientMasterWriteResponse](encodedBytes)
				if err != nil {
					t.Fatalf("DecodeMessage() error = %v", err)
				}
				original := message.(common.ClientMasterWriteResponse)
				if !reflect.DeepEqual(*decoded, original) {
					t.Errorf("Decoded message doesn't match original. Got %+v, want %+v", *decoded, original)
				}
				
			case common.PrimaryChunkCommitRequest:
				decoded, err := DecodeMessage[common.PrimaryChunkCommitRequest](encodedBytes)
				if err != nil {
					t.Fatalf("DecodeMessage() error = %v", err)
				}
				original := message.(common.PrimaryChunkCommitRequest)
				if !reflect.DeepEqual(*decoded, original) {
					t.Errorf("Decoded message doesn't match original. Got %+v, want %+v", *decoded, original)
				}
				
			case common.ClientChunkServerReadResponse:
				decoded, err := DecodeMessage[common.ClientChunkServerReadResponse](encodedBytes)
				if err != nil {
					t.Fatalf("DecodeMessage() error = %v", err)
				}
				original := message.(common.ClientChunkServerReadResponse)
				if !reflect.DeepEqual(*decoded, original) {
					t.Errorf("Decoded message doesn't match original. Got %+v, want %+v", *decoded, original)
				}
			}
		})
	}
}

// TestEncodeDecodeRoundTrip tests the full round trip of encoding and then decoding various messages
func TestEncodeDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		name        string
		messageType common.MessageType
		message     any
	}{
		{
			name:        "ClientMasterReadRequest",
			messageType: common.ClientMasterReadRequestType,
			message: common.ClientMasterReadRequest{
				Filename:   "test.txt",
				ChunkIndex: 5,
			},
		},
		{
			name:        "ClientMasterWriteResponse",
			messageType: common.ClientMasterWriteResponseType,
			message: common.ClientMasterWriteResponse{
				ChunkHandle:           123456789,
				MutationId:            987654321,
				PrimaryChunkServer:    "server1:8080",
				SecondaryChunkServers: []string{"server2:8080", "server3:8080"},
				ErrorMessage:          "test error",
			},
		},
		{
			name:        "ClientChunkServerWriteRequest",
			messageType: common.ClientChunkServerWriteRequestType,
			message: common.ClientChunkServerWriteRequest{
				MutationId:  12345,
				ChunkHandle: 67890,
			},
		},
		{
			name:        "MasterChunkServerHandshakeRequest",
			messageType: common.MasterChunkServerHandshakeRequestType,
			message: common.MasterChunkServerHandshakeRequest{
				ChunkHandles: []int64{100, 200, 300, 400, 500},
			},
		},
		{
			name:        "InterChunkServerCommitRequest",
			messageType: common.InterChunkServerCommitRequestType,
			message: common.InterChunkServerCommitRequest{
				ChunkHandle:   42,
				ChunkOffset:   100,
				MutationOrder: []int64{10, 20, 30, 40},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// First encode the message
			encoded, err := EncodeMessage(tt.messageType, tt.message)
			if err != nil {
				t.Fatalf("EncodeMessage() error = %v", err)
			}

			// Strip the header (message type and length) to get just the message body
			messageBytes := encoded[3:]

			// Then decode the message based on its type
			switch tt.messageType {
			case common.ClientMasterReadRequestType:
				decoded, err := DecodeMessage[common.ClientMasterReadRequest](messageBytes)
				if err != nil {
					t.Fatalf("DecodeMessage() error = %v", err)
				}
				original := tt.message.(common.ClientMasterReadRequest)
				if !reflect.DeepEqual(*decoded, original) {
					t.Errorf("Decoded message doesn't match original. Got %+v, want %+v", *decoded, original)
				}

			case common.ClientMasterWriteResponseType:
				decoded, err := DecodeMessage[common.ClientMasterWriteResponse](messageBytes)
				if err != nil {
					t.Fatalf("DecodeMessage() error = %v", err)
				}
				original := tt.message.(common.ClientMasterWriteResponse)
				if !reflect.DeepEqual(*decoded, original) {
					t.Errorf("Decoded message doesn't match original. Got %+v, want %+v", *decoded, original)
				}

			case common.ClientChunkServerWriteRequestType:
				decoded, err := DecodeMessage[common.ClientChunkServerWriteRequest](messageBytes)
				if err != nil {
					t.Fatalf("DecodeMessage() error = %v", err)
				}
				original := tt.message.(common.ClientChunkServerWriteRequest)
				if !reflect.DeepEqual(*decoded, original) {
					t.Errorf("Decoded message doesn't match original. Got %+v, want %+v", *decoded, original)
				}

			case common.MasterChunkServerHandshakeRequestType:
				decoded, err := DecodeMessage[common.MasterChunkServerHandshakeRequest](messageBytes)
				if err != nil {
					t.Fatalf("DecodeMessage() error = %v", err)
				}
				original := tt.message.(common.MasterChunkServerHandshakeRequest)
				if !reflect.DeepEqual(*decoded, original) {
					t.Errorf("Decoded message doesn't match original. Got %+v, want %+v", *decoded, original)
				}

			case common.InterChunkServerCommitRequestType:
				decoded, err := DecodeMessage[common.InterChunkServerCommitRequest](messageBytes)
				if err != nil {
					t.Fatalf("DecodeMessage() error = %v", err)
				}
				original := tt.message.(common.InterChunkServerCommitRequest)
				if !reflect.DeepEqual(*decoded, original) {
					t.Errorf("Decoded message doesn't match original. Got %+v, want %+v", *decoded, original)
				}
			}
		})
	}
}

// TestEdgeCases tests some edge cases for the encoding and decoding functions
func TestEdgeCases(t *testing.T) {
	// Test empty structures
	t.Run("EmptyStructTest", func(t *testing.T) {
		emptyStruct := common.MasterChunkServerHandshakeResponse{
			Message: "",
		}
		
		encoded, err := EncodeMessage(common.MasterChunkServerHandshakeResponseType, emptyStruct)
		if err != nil {
			t.Fatalf("Failed to encode empty struct: %v", err)
		}
		
		messageBytes := encoded[3:]
		decoded, err := DecodeMessage[common.MasterChunkServerHandshakeResponse](messageBytes)
		if err != nil {
			t.Fatalf("Failed to decode empty struct: %v", err)
		}
		
		if decoded.Message != "" {
			t.Errorf("Decoded empty struct has non-empty field: %v", decoded.Message)
		}
	})
	
	// Test large slices
	t.Run("LargeSliceTest", func(t *testing.T) {
		// Create a large slice of chunk handles
		largeChunkHandles := make([]int64, 1000)
		for i := 0; i < 1000; i++ {
			largeChunkHandles[i] = int64(i)
		}
		
		largeStruct := common.MasterChunkServerHandshakeRequest{
			ChunkHandles: largeChunkHandles,
		}
		
		encoded, err := EncodeMessage(common.MasterChunkServerHandshakeRequestType, largeStruct)
		if err != nil {
			t.Fatalf("Failed to encode large struct: %v", err)
		}
		
		messageBytes := encoded[3:]
		decoded, err := DecodeMessage[common.MasterChunkServerHandshakeRequest](messageBytes)
		if err != nil {
			t.Fatalf("Failed to decode large struct: %v", err)
		}
		
		if len(decoded.ChunkHandles) != 1000 {
			t.Errorf("Decoded large struct has wrong slice length: %v", len(decoded.ChunkHandles))
		}
		
		for i := 0; i < 1000; i++ {
			if decoded.ChunkHandles[i] != int64(i) {
				t.Errorf("Decoded large struct has wrong value at index %d: %v", i, decoded.ChunkHandles[i])
				break
			}
		}
	})
	
	// Test special characters in strings
	t.Run("SpecialCharactersTest", func(t *testing.T) {
		specialChars := common.ClientMasterReadRequest{
			Filename:   "特殊字符测试.txt",
			ChunkIndex: 42,
		}
		
		encoded, err := EncodeMessage(common.ClientMasterReadRequestType, specialChars)
		if err != nil {
			t.Fatalf("Failed to encode struct with special characters: %v", err)
		}
		
		messageBytes := encoded[3:]
		decoded, err := DecodeMessage[common.ClientMasterReadRequest](messageBytes)
		if err != nil {
			t.Fatalf("Failed to decode struct with special characters: %v", err)
		}
		
		if decoded.Filename != "特殊字符测试.txt" {
			t.Errorf("Decoded struct has wrong filename: %v", decoded.Filename)
		}
	})
}

// TestMessageTypeHeaderCorrectness tests that the message type header is correctly set in encoded messages
func TestMessageTypeHeaderCorrectness(t *testing.T) {
	testCases := []struct {
		messageType common.MessageType
		message     interface{}
	}{
		{common.ClientMasterReadRequestType, common.ClientMasterReadRequest{Filename: "test.txt", ChunkIndex: 1}},
		{common.ClientChunkServerWriteRequestType, common.ClientChunkServerWriteRequest{MutationId: 1, ChunkHandle: 2}},
		{common.MasterChunkServerLeaseRequestType, common.MasterChunkServerLeaseRequest{ChunkHandle: 3}},
	}

	for _, tc := range testCases {
		t.Run(MessageTypeToString(tc.messageType), func(t *testing.T) {
			encoded, err := EncodeMessage(tc.messageType, tc.message)
			if err != nil {
				t.Fatalf("EncodeMessage() error = %v", err)
			}

			// Check that the first byte contains the correct message type
			if common.MessageType(encoded[0]) != tc.messageType {
				t.Errorf("Message type header is incorrect. Got %v, want %v", 
					common.MessageType(encoded[0]), tc.messageType)
			}
		})
	}
}

// Helper function for converting a message type to a string
// Add this to your constants package
func MessageTypeToString(messageType common.MessageType) string {
	switch messageType {
	case common.ClientChunkServerReadRequestType:
		return "ClientChunkServerReadRequestType"
	case common.ClientChunkServerReadResponseType:
		return "ClientChunkServerReadResponseType"
	case common.ClientChunkServerWriteRequestType:
		return "ClientChunkServerWriteRequestType"
	case common.ClientChunkServerWriteResponseType:
		return "ClientChunkServerWriteResponseType"
	case common.ClientMasterReadRequestType:
		return "ClientMasterReadRequestType"
	case common.ClientMasterReadResponseType:
		return "ClientMasterReadResponseType"
	case common.ClientMasterWriteRequestType:
		return "ClientMasterWriteRequestType"
	case common.ClientMasterWriteResponseType:
		return "ClientMasterWriteResponseType"
	// Add other cases as needed
	default:
		return "UnknownMessageType"
	}
}