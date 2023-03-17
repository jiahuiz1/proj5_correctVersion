package surfstore

import (
	context "context"
	"fmt"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	block, ok := bs.BlockMap[blockHash.Hash]
	if !ok{
		// may change this to return not found block
		return &Block{BlockData: []byte{}, BlockSize: 0}, fmt.Errorf("block not found in the map")
	} else {
		return &Block{BlockData: block.BlockData, BlockSize: block.BlockSize}, nil
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	bs.BlockMap[GetBlockHashString(block.BlockData)] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	// check if this function works
	res := make([]string, 0)
	for _, h := range blockHashesIn.Hashes {
		if _, exists:= bs.BlockMap[h]; exists{
			res = append(res, h)
		}
	}
	return &BlockHashes{Hashes: res}, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	blockHashes := make([]string, 0)
	for k := range bs.BlockMap{
		blockHashes = append(blockHashes, k)
	}
	return &BlockHashes{Hashes: blockHashes}, nil
}


// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
