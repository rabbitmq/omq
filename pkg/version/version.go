package version

import "fmt"

var (
	Version string
	Commit  string
	Date    string
)

func Get() string {
	return fmt.Sprintf("OMQ %s, commit %s, built at %s", Version, Commit, Date)
}

func Print() {
	fmt.Println(Get())
}
