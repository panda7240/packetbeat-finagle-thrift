namespace java com.zhe800.finagle.thrift.result


struct FailDesc {
	1:string name,
	2:string failCode,
	3:string desc
}

struct Result {

	1:i32 code,

	2:optional list<FailDesc> failDescList
}

struct StringResult {
	1:Result result,

	2:optional string value,

	3:optional string extend
}