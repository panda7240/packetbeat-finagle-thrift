namespace java com.finagle.thrift.order


include "result.thrift"


/*订单*/
struct Order {

    1:i32 userId

	/*买家*/
	2:string userName,

	/*订单ID*/
	3:string orderId,
}



struct OrderResult {
    1:result.Result result,
	2:optional Order order
}









service OrderServ{


	/*订单详情*/
	OrderResult detail(1:i32 userId, 2:string userName, 3:string orderId)

	void finaglePing()

}
