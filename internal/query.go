package internal

type Query map[string]interface{}

func (q *Query) String() string {
	s, _ := jsonEncode(q)
	return s
}

func NewQuery() *Query {
	q := make(Query)
	return &q
}
