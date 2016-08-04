package finaglethrift

import (
	"fmt"
	"digger/packtbeat"
	"digger/utils"
)

var srvcSrvInstance *packtbeat.SrvcnameServer
var srvErr error

func initDigger(){
	srvcSrvInstance, srvErr = packtbeat.NewSrvcnameServer(utils.Bootup)
}

func LookupSrvcName(cip string, _cport string, sip string, _sport string, srverctype string) ([]string ,error) {
	if utils.Bootup {
		if srvErr != nil {
			fmt.Printf("Instance srvcnameServer occur err %v\n", srvErr)
			return []string{}, srvErr
		}
		resp, err := srvcSrvInstance.Lookup(cip, _cport, sip, _sport, srverctype)
		return resp, err
	}else {
		fmt.Println("Digger not start, withDigger=false")
		return []string{}, fmt.Errorf("Digger not start, withDigger=false ")
	}
}

