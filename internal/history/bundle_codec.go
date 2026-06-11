package history

import (
	"encoding/json"
	"fmt"

	"github.com/klauspost/compress/zstd"

	"github.com/boringsql/dryrun/internal/schema"
)

// wire format is JSON+zstd; keep encode/decode here so every backend emits identical bytes
func EncodeBundle(b *Bundle) ([]byte, error) {
	raw, err := json.Marshal(b)
	if err != nil {
		return nil, err
	}
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	defer enc.Close()
	return enc.EncodeAll(raw, nil), nil
}

// normalize nil Activity to empty so callers can index without a nil check
func DecodeBundle(raw []byte) (*Bundle, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	plain, err := dec.DecodeAll(raw, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress bundle: %w", err)
	}
	var b Bundle
	if err := json.Unmarshal(plain, &b); err != nil {
		return nil, fmt.Errorf("parse bundle: %w", err)
	}
	if b.Activity == nil {
		b.Activity = map[string]*schema.ActivityStatsSnapshot{}
	}
	return &b, nil
}
