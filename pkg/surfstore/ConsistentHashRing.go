package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"fmt"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	serverMap := c.ServerMap
	hashes := []string{}
	for h := range serverMap{
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)

	responsibleSer := ""
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			responsibleSer = serverMap[hashes[i]]
			break
		} 
	}

	if responsibleSer == ""{
		responsibleSer = serverMap[hashes[0]]
	}

	fmt.Println("Responsible server: ", responsibleSer)
	return responsibleSer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	serverMap := make(map[string]string)
	consistentHashRing := ConsistentHashRing{ServerMap: serverMap}
	for _, addr := range serverAddrs{
		hash := consistentHashRing.Hash("blockstore" + addr) // check if this is correct
		serverMap[hash] = "blockstore" + addr
	}
	return &ConsistentHashRing{ServerMap: serverMap}
}
