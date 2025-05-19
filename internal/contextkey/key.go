package contextkey

type ctxKey string

func (k ctxKey) String() string { return "langsmith-collector-proxy/" + string(k) }

const (
	ProjectIDKey ctxKey = "projectID"
	APIKeyKey    ctxKey = "apiKey"
)
