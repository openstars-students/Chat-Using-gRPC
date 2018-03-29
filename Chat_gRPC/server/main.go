package main

import (
	"fmt"
	"log"
	"net"
	pb "example/gRPC-Chat/Chat_gRPC"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
	bs "example/gRPC-Chat/Chat_gRPC/thrift/gen-go/generic"
	idbs "example/gRPC-Chat/Chat_gRPC/thrift/gen-go/idgenerate"
	sessionbs "example/gRPC-Chat/Chat_gRPC/thrift/gen-go/session"
	"crypto/sha1"
	"io"
	"sync"
	"strconv"
	//"github.com/constabulary/gb/testdata/src/a"
	"strings"
)

const address  ="127.0.0.1:8000"

type UserService struct{}

var clients = make(map[string]*Client)

type Client struct {
	uid string
	name      string
	ch        chan pb.Message
}


//dung de bam mat khau
func Hash(str string)string{
	n := sha1.New()
	n.Write([]byte(str))
	ns := n.Sum(nil)
	return string(ns)
}
//dang ki User
func (s *UserService) Register(ctx context.Context, in *pb.User) (*pb.Response, error) {

	fmt.Println("Register")

	//client: bigset data
	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()

	//idclient: bigset id
	idclient,_ := mpid.Get("127.0.0.1", "18405").Get()
	defer idclient.BackToPool()

	username := in.GetUsername()

	pass := Hash(in.GetPassword())
	var active string
	active = "0"

	if checkName(username){
		if checkPhone(in.GetPhone()){
			if checkEmail(in.GetEmail()){

				idclient.Client.(*idbs.TGeneratorClient).CreateGenerator("GenIdUserName")
				id := getValue("GenIdUserName")
				key_name := strconv.Itoa(int(id))

				fmt.Println("id: ",key_name, "username: ", in.GetUsername())

				//document Username_Id id special, it take username = key, key_name = value
				client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("UserName_Id", &bs.TItem{[]byte(string(in.GetUsername())),[]byte(string(key_name))})

				client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("UserName", &bs.TItem{[]byte(key_name),[]byte(in.GetUsername())})
				client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Password", &bs.TItem{[]byte(key_name),[]byte(pass)})
				client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Phone", &bs.TItem{[]byte(key_name),[]byte(in.GetPhone())})
				client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Email", &bs.TItem{[]byte(key_name),[]byte(in.GetEmail())})
				client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("CreatedTime", &bs.TItem{[]byte(key_name),[]byte(string(in.GetCreatedTime()))})
				client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Active", &bs.TItem{[]byte(key_name),[]byte(active)})
				return &pb.Response{Response:"Register Success"}, nil
			}else{return &pb.Response{Response:"that Email already exists"}, nil}
		}else {return &pb.Response{Response:"that Phone already exists"}, nil}
	}else{
		return &pb.Response{Response:"that Name already exists"},nil
	}
}

//login
func (s *UserService) Login(ctx context.Context, in *pb.UserLogin) (*pb.Response, error) {

	fmt.Println("Login: ")
	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()
	//get username, password
	username := in.GetUsername()
	password := Hash(in.GetPassword())


	//lay ra Uid tu username
	checkid,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsExisted("UserName_Id", []byte(username))
	if checkid.GetExisted() {

		id ,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("UserName_Id", []byte(username))
		key_id := string(id.Item.Value[:])

		name, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("UserName", []byte(key_id))
		checkname, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsExisted("UserName", []byte(key_id))

		if checkname.GetExisted() {

			//take id:
			pass, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Password", []byte(key_id))
			checkpass, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsExisted("Password", []byte(key_id))

			if checkpass.GetExisted() {
				if (string(pass.Item.Value[:]) == password && username == string(name.Item.Value[:])) {
					c := &Client{
						uid:  key_id,
						name: username,
						ch:   make(chan pb.Message, 100),
					}
					fmt.Println(c)

					//tao sessionkey
					ssclient, _ := mpcreatekey.Get("127.0.0.1", "19175").Get()
					defer ssclient.BackToPool()
					//session,err := client.Client.(*sessionbs.TSimpleSessionService_WClient).CreateSession(&c)
					//chuyen keyid into uid type i64
					uid, _ := strconv.ParseUint(key_id, 10, 64)

					user := sessionbs.TUserSessionInfo{
						Code:        1,
						ExpiredTime: 1,
						Permissions: "1",
						Version:     1,
						UID:         sessionbs.TUID(uid),
						Data:        password,
						DeviceInfo:  username,
					}
					session, _ := ssclient.Client.(*sessionbs.TSimpleSessionService_WClient).CreateSession(&user)
					var keysession sessionbs.TSessionKey
					keysession = session.GetSession()
					//key la uid o dang string

					clients[key_id] = c
					fmt.Println("key-id: ",len(clients))
					client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Active", &bs.TItem{[]byte(key_id),[]byte("1")})
					return &pb.Response{Response: string(keysession), Check: true}, nil
				}
			}
		}
	}
	return &pb.Response{Response:"Don't Success", Check: false},nil
}

//su nay sua in *pb.UserName thanh Uid
func (s *UserService) Logout(ctx context.Context, in *pb.Request) (*pb.Response, error){
	fromid,_ := checkSessionKey(in.GetSessionkey())
	if fromid!=0 {
		client, _ := mp.Get("127.0.0.1", "18407").Get()
		defer client.BackToPool()
		//client.Client.(*bs.TStringBigSetKVServiceClient).BsRemoveItem("Active", []byte(string(fromid)))
		//a,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Active", []byte(fromid))
		active := "0"
		client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Active", &bs.TItem{[]byte(strconv.Itoa(int(fromid))),[]byte(active)})

		delete(clients, strconv.FormatInt(fromid,10))
		return &pb.Response{Response: "Sussess Logout", Check:true}, nil
	}
	return &pb.Response{Response: "", Check:false}, nil
}

//truyen vao 1 key username, check xem co ton tai hay khong
func checkName(username string)bool{
	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()

	count := getCurrentId("GenIdUserName")
	//neu nhu co thi return false
	for i := 0; i<= int(count); i++{
		check, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsExisted("UserName", []byte(strconv.Itoa(i)))
		res,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("UserName", []byte(strconv.Itoa(i)))
		if check.Existed{
			if username == string(res.GetItem().GetValue()) {
				fmt.Println("check i: ", i)
				return false
			}
		}
	}
	return true
	//	User,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetSlice("test",0,int32( count))
}
//truyen vao 1 Phone, kiem tra xem da duoc dang ki chua
func checkPhone(phone string)bool{
	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()
	count := getCurrentId("GenIdUserName")
	//neu nhu co thi return false
	for i := 0; i<= int(count); i++{
		check, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsExisted("Phone", []byte(strconv.Itoa(i)))
		res,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Phone", []byte(strconv.Itoa(i)))
		if check.Existed{
			if phone == string(res.GetItem().GetValue()) {
				return false
			}
		}
	}
	return true
}
//truyen vao 1 email, check xem da duoc dang ki chua
func checkEmail(email string)bool{
	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()

	count := getCurrentId("GenIdUserName")
	//neu nhu co thi return false
	for i := 0; i<= int(count); i++{
		check, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsExisted("Email", []byte(strconv.Itoa(i)))
		res,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Email", []byte(strconv.Itoa(i)))
		if check.Existed{
			if email == string(res.GetItem().GetValue()) {
				return false
			}
		}
	}
	return true
}
//truyen vao sessionkey, tra ve stt, username
func checkSessionKey(sessionkey string) (int64, string){
	ssclient, _ := mpcreatekey.Get("127.0.0.1", "19175").Get()
	defer ssclient.BackToPool()
	//fmt.Println("session: ", sessionkey)

	uid,_ := ssclient.Client.(*sessionbs.TSimpleSessionService_WClient).GetSession(sessionbs.TSessionKey(sessionkey))

	if uid!= nil{return int64(uid.GetUserInfo().GetUID()),uid.GetUserInfo().GetDeviceInfo() } else {return 0,""}
}

//dua ra phan tu chung cua 2 mang
func list_Items_Common(arr []string,arr2 []string)  []string{

	lst :=[]string{}
	for _,item:=range arr{
		for _,item2:=range  arr2{
			if item==item2{
				lst = append(lst, item)
			}
		}
	}
	return lst
}
//tra ve uid chung
func checkIdConversation(lst []string) string{

	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()
	count,_ := client.Client.(*bs.TStringBigSetKVServiceClient).GetTotalCount("IdConversation")
	cid_common := ""
	dem :=0
	for i:=0 ; i < len(lst); i++{
		for j:=1; j<= int(count); j++{
			cid,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("IdConversation", []byte(strconv.Itoa(int(j))))
			cid1 := string(cid.Item.Value[:])
			if lst[i] == cid1{
				dem ++
			}
		}
		if dem ==2{
			cid_common = lst[i]
			break
		}
	}
	return cid_common
}
//truyen vao 2 uid, tra ve 1 cid
func (s *UserService) CreateConversation(ctx context.Context, in *pb.Request) (*pb.Response, error){
	fromid,_ := checkSessionKey(in.GetSessionkey())
	if fromid!=0 {
		idreceiver := in.GetId()

		client, _ := mp.Get("127.0.0.1", "18407").Get()
		defer client.BackToPool()
		//
		idclient,_ := mpid.Get("127.0.0.1", "18405").Get()
		defer idclient.BackToPool()

		//lay ra cid cua 2 uid
		lst_cid1 := get_cidConversationDetail(idreceiver)
		lst_cid2 := get_cidConversationDetail(strconv.FormatInt(fromid,10))

		fmt.Println(lst_cid1)
		fmt.Println(lst_cid2)
		//lay ra list cid chung
		lst_common := list_Items_Common(lst_cid1, lst_cid2)
		//lay ra cid chung
		get_cid := checkIdConversation(lst_common)

		if get_cid == "" {
			//gen id Conversation
			idclient.Client.(*idbs.TGeneratorClient).CreateGenerator("GenIdConversation")
			cid := getValue("GenIdConversation")

			//gen id conversationdetail
			//add uid, cid vao conversation lan 1
			idclient.Client.(*idbs.TGeneratorClient).CreateGenerator("GenIdConversationDetail")
			cdid := getValue("GenIdConversationDetail")

			client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("IdConversation", &bs.TItem{[]byte(strconv.Itoa(int(cdid))), []byte(strconv.Itoa(int(cid)))})
			client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("IdMember", &bs.TItem{[]byte(strconv.Itoa(int(cdid))), []byte(strconv.Itoa(int(fromid)))})

			//gen id conversationdetail
			//add uid, cid vao conversation lan 2
			cdid = getValue("GenIdConversationDetail")
			client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("IdConversation", &bs.TItem{[]byte(strconv.Itoa(int(cdid))), []byte(strconv.Itoa(int(cid)))})
			client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("IdMember", &bs.TItem{[]byte(strconv.Itoa(int(cdid))), []byte(idreceiver)})

			return &pb.Response{Id: strconv.Itoa(int(cid)), Check: true}, nil
		}else{
			return &pb.Response{Id: get_cid, Check: true}, nil
		}
	}
	return &pb.Response{Check:false}, nil
}

//tuyen vao uid, cid, tra ve true or false
func (s *UserService) AddUidToConversation(ctx context.Context, in *pb.ConversationDetail)(*pb.Response, error){
	return &pb.Response{},nil
}

//truyen vao 1 uid, tra ve list cid
func (s *UserService)GetAllConversation(ctx context.Context, in *pb.Request)(*pb.AllConversation, error){
	return &pb.AllConversation{},nil
}

func getMessValue(cid string){
	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()
	count,_ := client.Client.(*bs.TStringBigSetKVServiceClient).GetTotalCount("Content")
	for i:=1; i<=int(count); i++{

		key := strconv.Itoa(i)
		fromname, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Cid", []byte(key))
		Cid := string(fromname.Item.Value[:])
		if Cid == cid {
			fromname, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("FromName", []byte(key))
			fmt.Print(string(fromname.Item.Value[:]), " > ")
			content, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Content", []byte(key))
			if (content != nil && content.Item != nil && content.Item.Value != nil ) {
				fmt.Print( string(content.Item.Value[:]))
			}
			status, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("CheckMess", []byte(key))
			if (content != nil && content.Item != nil && content.Item.Value != nil ) {
				fmt.Println("   ",string(status.Item.Value[:]))
			}

		}
	}
}

//load tat ca cac tin nhan chua duoc nhan
//truyen vao sessionkey
func (s *UserService)LoadWaittingMess(ctx context.Context, in *pb.Request)(*pb.WaittingMessage, error){
	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()
	uid,_ := checkSessionKey(in.GetSessionkey())
	count,_ := client.Client.(*bs.TStringBigSetKVServiceClient).GetTotalCount("Content")

	mess := []pb.Message{}
	var m pb.Message

	lstmess := []*pb.Message{}

	if uid != 0 {
		//Mess,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetSlice("Content",0, int32(count))
		dem :=0
		for i := 1; i<=int(count); i++ {
			key := strconv.Itoa(i)
			check, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsExisted("ToId", []byte(key))
			if check.GetExisted() {
				//tim ToId
				toid, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("ToId", []byte(key))
				ToId := string(toid.Item.Value[:])
				//check status message
				checkmess, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("CheckMess", []byte(key))
				CheckMess := string(checkmess.Item.Value[:])
				//neu ToId == Uid va status mess chua duoc gui
				if strconv.Itoa(int(uid)) == ToId && CheckMess == "0" {
					fmt.Println("i= ", i)
					fmt.Println(" dem", dem)
					//lay content
					content,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Content", []byte(key))
					m.Content = string(content.Item.Value[:])
					//lay time
					createdtime, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("MessCreatedTime", []byte(key))
					m.CreatedTime = string(createdtime.Item.Value[:])
					//lay fromname
					fromname, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("FromName", []byte(key))
					m.FromName = string(fromname.Item.Value[:])
					//thuoc cid nao
					cid, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Cid", []byte(key))
					m.Cid = string(cid.Item.Value[:])

					client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("CheckMess", &bs.TItem{[]byte(key),[]byte("1")})

					mess = append(mess, m)

					lstmess =append(lstmess, &mess[dem])
					dem ++
				}
			}
		}
		return &pb.WaittingMessage{Waittingmess:lstmess}, nil
	} else {return &pb.WaittingMessage{Waittingmess:lstmess }, nil}
}

//tra ve tat ca cac tin nhan theo Cid
/*
func (s *UserService) LoadAllMess(ctx context.Context, in *pb.Request) (*pb.AllMessages, error){
	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()
	uid,_ := checkSessionKey(in.GetSessionkey())
	count,_ := client.Client.(*bs.TStringBigSetKVServiceClient).GetTotalCount("Content")

	mess := []pb.Message{}
	var m pb.Message
	lstmess := []*pb.Message{}

	if uid != 0 {
		//Mess,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetSlice("Content",0, int32(count))
		dem :=0
		for i := 1; i<=int(count); i++ {
			key := strconv.Itoa(i)
			checkCid, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsExisted("Cid", []byte(key))
			if checkCid.GetExisted() {
				//get cid
				Cid, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Cid", []byte(key))
				cid := string(Cid.Item.Value[:])

				if cid == in.GetRequest(){
					//lay content
					content, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Content", []byte(key))
					m.Content = string(content.Item.Value[:])
					//lay time
					createdtime, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("MessCreatedTime", []byte(key))
					m.CreatedTime = string(createdtime.Item.Value[:])
					//lay fromname
					fromname, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("FromName", []byte(key))
					m.FromName = string(fromname.Item.Value[:])

					mess = append(mess, m)
					lstmess = append(lstmess, &mess[dem])
					dem ++
				}
			}
		}
		return &pb.AllMessages{Allmess:lstmess}, nil
	} else {return &pb.AllMessages{Allmess:lstmess }, nil}

}
*/

//tra ve tat ca cac tin nhan theo Cid
func (s *UserService) LoadAllMessOnCid(ctx context.Context, in *pb.Request) (*pb.AllMessages, error) {
	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()
	uid, _ := checkSessionKey(in.GetSessionkey())
	//count, _ := client.Client.(*bs.TStringBigSetKVServiceClient).GetTotalCount("Content")

	mess := []pb.Message{}
	var m pb.Message
	lstmess := []*pb.Message{}

	cidRequest := in.GetRequest()
	if uid != 0 {
		//Mess,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetSlice("Content",0, int32(count))
		dem := 0
		checkCidRequest, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsExisted("Cid", []byte(cidRequest))
		if checkCidRequest.GetExisted() {
			str, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Cid-nMessId", []byte(cidRequest))
			//s := string(str.Item.Value[:])
			s := strings.Split(string(str.Item.Value[:]), " ")
			for _, mid := range s {
				content, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Content", []byte(mid))
				m.Content = string(content.Item.Value[:])
				//lay time
				createdtime, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("MessCreatedTime", []byte(mid))
				m.CreatedTime = string(createdtime.Item.Value[:])
				//lay fromname
				fromname, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("FromName", []byte(mid))
				m.FromName = string(fromname.Item.Value[:])

				mess = append(mess, m)
				lstmess = append(lstmess, &mess[dem])
				dem ++
			}
		}
		return &pb.AllMessages{Allmess:lstmess}, nil
	} else {return &pb.AllMessages{Allmess:lstmess }, nil}
}
//add friend, truyen vao 1 sessionkey, uid_B
func (s *UserService) AddFriend(ctx context.Context, in *pb.Request) ( *pb.Response, error) {
	return &pb.Response{},nil
}
//lay danh sach tat ca user
func (s *UserService) GetListUser(ctx context.Context, in *pb.Request)(*pb.AllInfoUser, error) {

	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()
	uid,_ := checkSessionKey(in.GetSessionkey())
	count,_ := client.Client.(*bs.TStringBigSetKVServiceClient).GetTotalCount("UserName")

	user := []pb.User{}
	var m pb.User
	listuser := []*pb.User{}

	if uid != 0 {
		//Mess,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetSlice("Content",0, int32(count))
		dem :=0
		for i := 1; i<=int(count); i++ {
			key := strconv.Itoa(i)
			check, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsExisted("UserName", []byte(key))
			if check.GetExisted() {
				//tim ToId
				username, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("UserName", []byte(key))
				UserName := string(username.Item.Value[:])
				m.Username = UserName
				m.Uid = uint64(i)

				active, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Active", []byte(key))
				Active := string(active.Item.Value[:])

				if Active =="1" {
					m.Active = true
				}else {m.Active =false}

				user = append(user, m)
				listuser =append(listuser, &user[dem])
				dem ++
			}
		}
		return &pb.AllInfoUser{Alluser:listuser}, nil
	} else {return &pb.AllInfoUser{Alluser:listuser }, nil}
}
//lay danh sach ban be
func (s *UserService) GetListFriend(ctx context.Context, in *pb.Request)(*pb.AllInfoUser, error) {
	return &pb.AllInfoUser{}, nil
}

//truyen vao username, tra ve uid
func(s *UserService) GetId(ctx context.Context, req *pb.Request)(*pb.Response, error) {

	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()
	uid, _ := checkSessionKey(req.GetSessionkey())
	if uid != 0 {
		username := req.GetRequest()
		checkId,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsExisted("UserName_Id", []byte(username))
		if checkId.GetExisted() {
			id, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("UserName_Id", []byte(username))
			uid := string(id.Item.Value[:])
			return &pb.Response{Check: true, Id: uid}, nil
		}else{
			return &pb.Response{Check:false}, nil
		}
	} else{
		return &pb.Response{Check:false}, nil
	}
}
//luu tin nhan vao trong csdl
func saveMessage(mess pb.Message){
	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()
	//sinh mid
	idclient,_ := mpid.Get("127.0.0.1", "18405").Get()
	defer idclient.BackToPool()
	idclient.Client.(*idbs.TGeneratorClient).CreateGenerator("GenIdMessage")
	id := getValue("GenIdMessage")
	mid := strconv.Itoa(int(id))
	var checkmess string
	//check xem tin nhan da duoc gui thanh cong hay chua
	if mess.Check{
		checkmess ="1"
	}else{checkmess ="0"}

	fromid,_ := checkSessionKey(mess.GetSessionkey())

	client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Content", &bs.TItem{[]byte(mid),[]byte(mess.Content)})
	client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("FromId", &bs.TItem{[]byte(mid),[]byte(strconv.FormatInt(fromid,10))})
	//client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("ConversationId", &bs.TItem{[]byte(mid),[]byte(strconv.FormatInt(fromid,10))})
	client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("ToId", &bs.TItem{[]byte(mid),[]byte(mess.ToUid)})
	client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("MessCreatedTime", &bs.TItem{[]byte(mid),[]byte(mess.CreatedTime)})
	client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("CheckMess", &bs.TItem{[]byte(mid),[]byte(checkmess)})

	client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Cid", &bs.TItem{[]byte(mid),[]byte(mess.GetCid())})
	client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("FromName", &bs.TItem{[]byte(mid),[]byte(mess.FromName)})

	a, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Content", []byte(mid))
	b := a.Item.Value[:]
	fmt.Print("Cid: ",string(mess.Cid))
	count,_ := client.Client.(*bs.TStringBigSetKVServiceClient).GetTotalCount("Content")
	fmt.Println(" content:= ",string(b),"   messId := ", mid, "  count: ", count, "toid: ", mess.ToUid, "check: ", checkmess	)

	check,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsExisted("Cid-nMessId", []byte(mess.GetCid()))
	if check.GetExisted(){
		str,_:= client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Cid-nMessId", []byte(mess.GetCid()))
		s := string(str.Item.Value[:])
		s = s + " " + mid
		fmt.Println(s)
		client.Client.(*bs.TStringBigSetKVServiceClient).BsRemoveItem("Cid-nMessId", []byte(mess.GetCid()))
		client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Cid-nMessId", &bs.TItem{[]byte(mess.GetCid()),[]byte(s)})
	} else {
		client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Cid-nMessId", &bs.TItem{[]byte(mess.GetCid()), []byte(mid)})
	}
}

//check xem User co ton tai ko
func (s *UserService) CheckUser(ctx context.Context, in *pb.Request) (*pb.Response, error) {
	//truoc tien check xem no co online
	uid,_ := checkSessionKey(in.GetSessionkey())
	if uid != 0 {
		var username= clients[in.GetRequest()]
		if username == nil {
			return &pb.Response{Response: "UserName Not Exited", Check: false}, nil
		} else {
			return &pb.Response{Response: "UserName Exited", Check: true}, nil
		}
	}else {return &pb.Response{Response: "UserName Not Exited", Check: false}, nil}
}

//check xem nguoi nhan co online hay khong
func messageWatting(mess pb.Message) bool{
	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()
	//neu trong database ko co du lieu thi phai check xem no ton tai ko, neu ko thi se gay ta loi
	check, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsExisted("UserName", []byte(mess.ToUid))
	if check.Existed{
		//kiem tra no co online ko
		var username = clients[mess.ToUid]
		//neu ko online thi tra ve true
		if username == nil {
			return true
		}else{return false}
	}else {return false}
}

//lang nghe tin nhan den
func listenToClient(stream pb.ChatgRPC_RouteChatServer, messages chan<- pb.Message, wg sync.WaitGroup, fromname string) {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			fmt.Println("err == io.EOF")
			defer wg.Done()
			return
		}
		if err != nil {
			fmt.Println("err != nil")

			client, _ := mp.Get("127.0.0.1", "18407").Get()
			defer client.BackToPool()
			id, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("UserName_Id", []byte(fromname))
			uid := string(id.Item.Value[:])
			fmt.Println(uid)
			client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Active", &bs.TItem{[]byte(uid),[]byte("0")})
			fmt.Println(len(clients))
			delete(clients, uid)
			defer wg.Done()
			return
		} else {msg.FromName = fromname ; messages <- *msg}
	}
	defer wg.Done()
}
//truyen vao 1 uid, tra ve 1 mang cid thuoc cid do
func get_cidConversationDetail(Uid string) (cids []string){

	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()
	count,_ := client.Client.(*bs.TStringBigSetKVServiceClient).GetTotalCount("IdConversation")

	cids = []string{}
	for i:=1; i<= int(count); i++{
		uid,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("IdMember", []byte(strconv.Itoa(int(i))))
		uid1 := string(uid.Item.Value[:])
		if (uid1 == Uid){
			cid,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("IdConversation", []byte(strconv.Itoa(int(i))))
			cids = append(cids,string(cid.Item.Value[:]))
		}
	}
	return cids
}
//truyen vao 1 cid, tra ve 1 mang uid thuoc cid do
func get_uidConversationDetail(Cid string) (uids []string){

	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()
	count,_ := client.Client.(*bs.TStringBigSetKVServiceClient).GetTotalCount("IdConversation")

	uids = []string{}
	for i:=1; i<= int(count); i++{
		cid,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("IdConversation", []byte(strconv.Itoa(int(i))))
		cid1 := string(cid.Item.Value[:])
		if (cid1 == Cid){
			uid,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("IdMember", []byte(strconv.Itoa(int(i))))
			uids = append(uids,string(uid.Item.Value[:]))
		}
	}
	return uids
}

//gui tin cho cac user trong group
func broadcast(fromid string, cid string, msg pb.Message) {
	var uids []string
	uids = []string{}
	uids = get_uidConversationDetail(cid)
	//gui tin nhan cho cac uid trong cid
	for _,uid := range uids {
		//	fmt.Println("uid:  ", uid)
		if fromid != uid   {

			//check nguoi nhan co online hay ko, gan "msg.ToUid = uid" de check nguoi nhan
			msg.ToUid = uid
			if messageWatting(msg){
				//trang thai tin nhan chua duoc gui cho ng nhan
				msg.Check = false
				saveMessage(msg )
				return
			}

			msg.ToUid = cid
			clients[uid].ch <- msg
		}
	}
}
//tin nhan den bao gom fromid, cid
//tin nhan tra ve bao gom fromname, cid
func (s *UserService)RouteChat(stream pb.ChatgRPC_RouteChatServer) error {
	fmt.Println("RouteChat: ")
	var wg sync.WaitGroup
	wg.Add(1)
	mess,_ := stream.Recv()
	//sau khi nhan thi stream se tro thanh ko co gia tri.
	//nhan hang ngan tu thi cung the ca thoi
	//tra ve id, name
	from_id,from_name := checkSessionKey(mess.GetSessionkey())
	if from_id !=0{
		//check xem nguoi nhan co online khong, neu khong thi save tin nhan
		if messageWatting(*mess){
			//trang thai tin nhan chua duoc gui cho ng nhan
			mess.Check = false
			mess.FromName = from_name
			saveMessage(*mess )
			return nil
		}
		clientMessages := make(chan pb.Message)
		go listenToClient(stream, clientMessages,wg, from_name)

		for {
			select {
			case messageFromClient := <-clientMessages:
				broadcast(strconv.Itoa(int(from_id)),mess.GetCid(), messageFromClient)
				break
			case messageFromOthers := <-clients[strconv.Itoa(int(from_id))].ch:
				err := stream.Send(&messageFromOthers)
				if err ==nil{
					//fmt.Println("content: ", messageFromOthers.Content, "   check: ", messageFromOthers.Check)
					messageFromOthers.Check = true
					saveMessage(messageFromOthers)
				}
			}
		}

	}else {return nil}
}
func (s *UserService) GetInfoUser(ctx context.Context, in *pb.Request) (*pb.User, error) {

	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()

	//truoc tien check xem no co online
	idsender,_ := checkSessionKey(in.GetSessionkey())
	var user pb.User
	if idsender != 0 {
		key := in.GetRequest()

		createdtime, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("UserName", []byte(key))
		user.Username= string(createdtime.Item.Value[:])

		active,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Active",[]byte(key))
		if string(active.Item.Value[:]) =="1"{
			user.Active = true
		}else {user.Active=false}
		return &user, nil
	}else {return &user, nil}
}
func a(){
	client, _ := mp.Get("127.0.0.1", "18407").Get()
	defer client.BackToPool()
	var c bs.TItemSet

	var item bs.TItem
	item.Key= []byte("3")
	item.Value = []byte("c")

	c.Items = append(c.Items,&item)
	// ,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsBulkLoad("UserName", &c)
	a ,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsMultiPut("UserName", &c,false, false)

	fmt.Println(a)
}

func main(){
	listen, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	pb.RegisterChatgRPCServer(s, &UserService{})
	//	a()
	fmt.Println("Listening on the 0.0.0.0:8000")
	if err := s.Serve(listen); err != nil {
		log.Fatal(err)
	}
}

