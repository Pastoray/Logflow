// Command healthcheck is a tiny, dependency-free HTTP probe used as the
// container HEALTHCHECK for logflow agent containers. The runtime image is
// built FROM scratch (see Dockerfile) and so has no shell, curl, or wget to
// run a conventional `wget -qO- http://...` healthcheck with - this binary
// is the replacement: it GETs a URL and exits 0 on any 2xx response, 1
// otherwise (including on error or timeout).
package main

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: healthcheck <url>")
		os.Exit(2)
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck: ", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "healthcheck: unhealthy status %d\n", resp.StatusCode)
		os.Exit(1)
	}
}
