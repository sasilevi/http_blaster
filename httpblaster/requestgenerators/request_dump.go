package requestgenerators

//RequestDump : request data structure to dump to file
type RequestDump struct {
	Host    string
	Method  string
	URI     string
	Body    string
	Headers map[string]string
}
