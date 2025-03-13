package master

import (
	"math/rand/v2"
)

func (master *Master) chooseSecondaryServers() []string {
	servers := make([]string, 0)
	for i := range 3 {
		server := master.serverList[i]
		servers = append(servers, server.server)
		master.serverList.update(server, server.NumberOfChunks+1)
	}

	return servers
}

func (master *Master) generateNewChunkId() int64 {
	id := master.idGenerator.Generate()
	return id.Int64()
}

func(master *Master) choosePrimaryAndSecondary(servers []string) (string,[]string){
	rand.Shuffle(len(servers), func(i, j int) {
		servers[i], servers[j] = servers[j], servers[i]
	})

	primaryServer := servers[0] // First server after shuffling is primary

	var secondaryServers []string
	if len(servers) > 1 {
		secondaryServers = servers[1:] // Remaining servers after shuffling are secondaries
	} else {
		secondaryServers = []string{} // No secondary servers
	}

	return primaryServer, secondaryServers
}