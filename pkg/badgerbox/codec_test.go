package badgerbox

import "testing"

func TestJSONCodecRoundTrip(t *testing.T) {
	t.Parallel()

	codec := JSONCodec[testPayload]{}
	encoded, err := codec.Marshal(testPayload{Name: "hello"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	decoded, err := codec.Unmarshal(encoded)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Name != "hello" {
		t.Fatalf("decoded payload mismatch: %#v", decoded)
	}
}
