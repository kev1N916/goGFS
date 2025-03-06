package main



type Client struct{
	masterServer string
	chunkCache map[string][]string
}

func NewClient (masterServer string) *Client{
	return &Client{
		masterServer: masterServer,
	}
}


func main() {
}


