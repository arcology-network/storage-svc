package types

type DB interface {
	Set(string, []byte) error
	Get(string) ([]byte, error)
	BatchSet([]string, [][]byte) error
	BatchGet([]string) ([][]byte, error)
}
