package main


func main() {
	chunkServer:=NewChunkServer("8081")
	chunkServer.loadChunks()
	go chunkServer.start()
}