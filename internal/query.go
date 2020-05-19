package internal

// Query es查询的 query
type Query map[string]interface{}

func (q *Query) String() string {
	s, _ := jsonEncode(q)
	return s
}

// NewQuery 创建一个空的query
func NewQuery() *Query {
	q := make(Query)
	return &q
}
