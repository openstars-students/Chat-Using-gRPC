package main

import (
	"fmt"
	"log"
	"net"
	pb "example/Chat-Using-gRPC/Chat_gRPC"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
	bs "example/Chat-Using-gRPC/Chat_gRPC/thrift/gen-go/generic"
	idbs "example/Chat-Using-gRPC/Chat_gRPC/thrift/gen-go/idgenerate"
	sessionbs "example/Chat-Using-gRPC/Chat_gRPC/thrift/gen-go/session"
	"crypto/sha1"
	"io"
	"sync"
	"strconv"
	//"github.com/constabulary/gb/testdata/src/a"
	"strings"
	"time"
)

const address  ="192.168.0.6:8000"


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
//dang ki User
func (s *UserService) Register(ctx context.Context, in *pb.User) (*pb.Response, error) {

	client, _ := mp.Get("192.168.0.6", "18407").Get()
	defer client.BackToPool()
	//idclient: bigset id
	idclient,_ := mpid.Get("192.168.0.6", "18405").Get()
	defer idclient.BackToPool()
	username := in.GetUsername()
	pass := Hash(in.GetPassword())

	if checkName(username){
		if checkPhone(in.GetPhone()){
			if checkEmail(in.GetEmail()){
				idclient.Client.(*idbs.TGeneratorClient).CreateGenerator("GenIdUserName")
				id := getValue("GenIdUserName")
				key_name := strconv.Itoa(int(id))

				fmt.Println("id: ",key_name, "username: ", in.GetUsername())
				client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("UserName_Id", &bs.TItem{[]byte(string(in.GetUsername())),[]byte(string(key_name))})

				client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("UserName", &bs.TItem{[]byte(key_name),[]byte(in.GetUsername())})
				t := strconv.Itoa(int(time.Now().Unix()))
				client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Active", &bs.TItem{[]byte(key_name),[]byte("0")})
				str := pass + " " + in.GetEmail() + " " + in.GetPhone() + " " + t
				client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("InfoUser", &bs.TItem{[]byte(key_name),[]byte(str)})

				client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Avatar", &bs.TItem{[]byte(key_name),in.GetAvatar()})

				log.Println("[Register]:Register Success " + in.GetUsername() + str )
				return &pb.Response{Response:"Register Success"}, nil
			}else{return &pb.Response{Response:"that Email already exists"}, nil}
		}else {return &pb.Response{Response:"that Phone already exists"}, nil}
	}else{
		return &pb.Response{Response:"that Name already exists"},nil
	}
}

func (s *UserService) Login(ctx context.Context, in *pb.UserLogin) (*pb.Response, error) {

	client, _ := mp.Get("192.168.0.6", "18407").Get()
	defer client.BackToPool()
	//get username, password
	username := in.GetUsername()
	password := Hash(in.GetPassword())

	id ,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("UserName_Id", []byte(username))
	if id != nil && id.Item !=nil{
		key_id := string(id.Item.Value[:])
		name, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("UserName", []byte(key_id))
		if name != nil && name.Item !=nil{
			res,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("InfoUser", []byte(key_id))
			if res != nil && res.Item !=nil{
				s := string(res.GetItem().GetValue())
				str := strings.Split(s, " ")
				if (str[0] == password && username == string(name.Item.Value[:])) {
					c := &Client{
						uid:  key_id,
						name: username,
						ch:   make(chan pb.Message, 100),
					}
					//tao sessionkey
					ssclient, _ := mpcreatekey.Get("192.168.0.6", "19175").Get()
					defer ssclient.BackToPool()
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
					client.Client.(*bs.TStringBigSetKVServiceClient).BsRemoveItem("Active",[]byte(key_id))
					client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Active", &bs.TItem{[]byte(key_id),[]byte("1")})
					log.Println("[Login]:Login Success " + in.GetUsername() + "$$$" + password)
					return &pb.Response{Response: string(keysession), Check: true}, nil
				}
			}
		}
	}
	log.Println("[Login]:Login Don't Success " + in.GetUsername() + "$$$" + in.GetPassword())
	return &pb.Response{Response:"Don't Success", Check: false},nil
}

//su nay sua in *pb.UserName thanh Uid
func (s *UserService) Logout(ctx context.Context, in *pb.Request) (*pb.Response, error){
	fromid,_ := checkSessionKey(in.GetSessionkey())
	if fromid!=0 {
		client, _ := mp.Get("192.168.0.6", "18407").Get()
		defer client.BackToPool()
		active := "0"
		client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Active", &bs.TItem{[]byte(strconv.Itoa(int(fromid))),[]byte(active)})
		return &pb.Response{Response: "Sussess Logout", Check:true}, nil
	}
	return &pb.Response{Response: "", Check:false}, nil
}

//truyen vao 1 key username, check xem co ton tai hay khong
func checkName(username string)bool{
	client, _ := mp.Get("192.168.0.6", "18407").Get()
	defer client.BackToPool()

	count := getCurrentId("GenIdUserName")
	//neu nhu co thi return false
	for i := 0; i<= int(count); i++{
		res,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("UserName", []byte(strconv.Itoa(i)))
		if res != nil && res.Item != nil{
			if username == string(res.GetItem().GetValue()) {
				log.Println("[checkName]:Username Existe: "+ username)
				return false
			}
		}
	}
	log.Println("[checkName]:Username Not Existed: "+ username)
	return true
}
//truyen vao 1 Phone, kiem tra xem da duoc dang ki chua
func checkPhone(phone string)bool{
	client, _ := mp.Get("192.168.0.6", "18407").Get()
	defer client.BackToPool()
	count := getCurrentId("GenIdUserName")
	//neu nhu co thi return false
	for i := 0; i<= int(count); i++{
		res,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("InfoUser", []byte(strconv.Itoa(i)))
		if res != nil && res.Item != nil{
			s := string(res.GetItem().GetValue())
			str := strings.Split(s, " ")
			if phone == str[2] {
				log.Println("[checkPhone]:Phone Existed: "+ phone)
				return false
			}
		}
	}
	log.Println("[checkPhone]:Phone Not Existed: "+ phone)
	return true
}
//truyen vao 1 email, check xem da duoc dang ki chua
func checkEmail(email string)bool{
	client, _ := mp.Get("192.168.0.6", "18407").Get()
	defer client.BackToPool()

	count := getCurrentId("GenIdUserName")
	//neu nhu co thi return false
	for i := 0; i<= int(count); i++{
		res,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("InfoUser", []byte(strconv.Itoa(i)))
		if res != nil && res.Item != nil{
			s := string(res.GetItem().GetValue())
			str := strings.Split(s, " ")
			if email == str[1] {
				log.Println("[checkEmail]:Email Existed: "+ email)
				return false
			}
		}
	}
	log.Println("[checkEmail]:Email not Existed: "+ email)
	return true
}
//truyen vao sessionkey, tra ve stt, username
func checkSessionKey(sessionkey string) (int64, string) {
	ssclient, _ := mpcreatekey.Get("192.168.0.6", "19175").Get()
	defer ssclient.BackToPool()
	uid,_ := ssclient.Client.(*sessionbs.TSimpleSessionService_WClient).GetSession(sessionbs.TSessionKey(sessionkey))
	if uid != nil && uid.GetUserInfo() != nil {
		log.Println("[checkSessionKey]:SessionKey Existed: ",uid.GetUserInfo().GetDeviceInfo())
		return int64(uid.GetUserInfo().GetUID()), uid.GetUserInfo().GetDeviceInfo()
	} else {
		log.Println("[checkSessionKey]:SessionKey not Existed: ",uid.GetUserInfo().GetDeviceInfo())
		return 0, ""
	}
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
	log.Println("[list_Items_Common]:list Items Common: ",lst)
	return lst
}

func (s *UserService) CreateConversation(ctx context.Context, in *pb.Request) (*pb.Response, error){

	fromid,_ := checkSessionKey(in.GetSessionkey())
	if fromid!=0 {
		log.Println("[CreateConversation]:fromid ",fromid)
		client, _ := mp.Get("192.168.0.6", "18407").Get()
		defer client.BackToPool()
		//
		idclient, _ := mpid.Get("192.168.0.6", "18405").Get()
		defer idclient.BackToPool()
		idreceiver := in.GetRequest()

		s := strings.Split(idreceiver, "$$$")

		if len(s) == 1 {
			str := strconv.FormatInt(fromid, 10) + "$$$" + idreceiver

			log.Println("[CreateConversation]:chuoi uid ",str)
			var  listCid1 []string
			getCidfromUid1,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Uid-nCid",[]byte(strconv.Itoa(int(fromid))))
			if getCidfromUid1 != nil && getCidfromUid1.Item != nil{
				s1 := string(getCidfromUid1.GetItem().GetValue())
				listCid1 = strings.Split(s1,"$$$")
			}
			getCidfromUid2,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Uid-nCid",[]byte(idreceiver))
			var  listCid2 []string
			if getCidfromUid2 != nil && getCidfromUid2.Item != nil{
				s2 := string(getCidfromUid2.GetItem().GetValue())
				listCid2 = strings.Split(s2,"$$$")
			}

			log.Println("[CreateConversation]:list Cid of fromid: ",listCid1)
			log.Println("[CreateConversation]:list Cid of idrevicer: ",listCid2)
			lst_common := list_Items_Common(listCid1, listCid2)
			for _, cid := range lst_common{
				str,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Cid-nUid",[]byte(cid))
				if str != nil && str.Item != nil{
					strSplit := strings.Split(string(str.GetItem().GetValue()),"$$$")
					if len(strSplit) == 2{
						log.Println("[CreateConversation]:Cid: ",cid)
						return &pb.Response{Response:cid, Check: true}, nil
					}
				}
			}

			idclient.Client.(*idbs.TGeneratorClient).CreateGenerator("GenIdConversation")
			cid := getValue("GenIdConversation")
			log.Println("[CreateConversation]:GenIdConversation: ",cid)

			client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Cid-nUid", &bs.TItem{[]byte(strconv.Itoa(int(cid))), []byte(str)})

			array_str := strings.Split(str,"$$$")
			for _, uid := range array_str{
				getListCid,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Uid-nCid",[]byte(uid))
				if getListCid != nil && getListCid.Item != nil{
					x := string(getListCid.GetItem().GetValue())
					x = x + "$$$" + strconv.FormatInt(cid,10)

					client.Client.(*bs.TStringBigSetKVServiceClient).BsRemoveItem("Uid-nCid", []byte(uid))
					client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Uid-nCid", &bs.TItem{[]byte(uid), []byte(x)})
				}else{
					client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Uid-nCid", &bs.TItem{[]byte(uid), []byte(strconv.FormatInt(cid,10))})
				}

			}
			return &pb.Response{Response: strconv.Itoa(int(cid)), Check: true}, nil
		}else{

			idclient.Client.(*idbs.TGeneratorClient).CreateGenerator("GenIdConversation")
			cid := getValue("GenIdConversation")
			log.Println("[CreateConversation]:GenIdConversation: ",cid)
			idreceiver = idreceiver + "$$$" + strconv.FormatInt(fromid,10)
			client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Cid-nUid", &bs.TItem{[]byte(strconv.Itoa(int(cid))), []byte(idreceiver)})

			s = append(s,strconv.FormatInt(fromid,10))
			for _, uid := range s{
				str,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Uid-nCid",[]byte(uid))
				if str != nil && str.Item != nil{
					x := string(str.GetItem().GetValue())
					x = x + "$$$" + strconv.FormatInt(cid,10)

					client.Client.(*bs.TStringBigSetKVServiceClient).BsRemoveItem("Uid-nCid", []byte(uid))
					client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Uid-nCid", &bs.TItem{[]byte(uid), []byte(x)})
				}
			}
			return &pb.Response{Response: strconv.Itoa(int(cid)), Check: true}, nil
		}
	}

	log.Println("[CreateConversation]:CreateConversation id Fail: ")
	return &pb.Response{Check:false}, nil
}

//tuyen vao n uid, cid, tra ve true or false
func (s *UserService) AddUidToConversation(ctx context.Context, in *pb.Request)(*pb.Response, error) {

	client, _ := mp.Get("192.168.0.6", "18407").Get()
	defer client.BackToPool()
	uid, _ := checkSessionKey(in.GetSessionkey())

	cidRequest := in.GetRequest()
	uidRequest := in.GetId()

	if uid != 0 {

		getCidfromUid1,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Cid-nUid",[]byte(cidRequest))
		if getCidfromUid1 != nil && getCidfromUid1.Item != nil{
			s1 := string(getCidfromUid1.GetItem().GetValue())

			if !strings.Contains(s1, uidRequest){
				s1 = s1 + "$$$" + uidRequest
				log.Println("[CreateConversation]:array cid: ",s1)
				client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Cid-nUid", &bs.TItem{[]byte(cidRequest),[]byte(s1)})
				return &pb.Response{Response: "Success", Check: true}, nil
			}
		}
		return &pb.Response{Response: "Fails", Check: false}, nil
	}
	return &pb.Response{Response: "Fails", Check: false}, nil
}
//truyen vao 1 uid, tra ve list cid
func (s *UserService)GetAllConversation(ctx context.Context, in *pb.Request)(*pb.AllConversation, error){

	fmt.Println("GetAllConversation: ")
	client, _ := mp.Get("192.168.0.6", "18407").Get()
	defer client.BackToPool()
	fromid, _ := checkSessionKey(in.GetSessionkey())

	if fromid != 0 {
		listCid := []*pb.Conversation{}
		var lst_cid []string

		getCidfromUid1,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Uid-nCid",[]byte(strconv.FormatInt(fromid,10)))
		if getCidfromUid1 != nil && getCidfromUid1.Item != nil{
			s1 := string(getCidfromUid1.GetItem().GetValue())
			lst_cid = strings.Split(s1,"$$$")
		}

		for _, cid := range lst_cid{
			Cid := pb.Conversation{}
			Cid.Cid = cid

			var uids []string
			getCidfromUid1,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Cid-nUid",[]byte(cid))
			if getCidfromUid1 != nil && getCidfromUid1.Item != nil{
				s1 := string(getCidfromUid1.GetItem().GetValue())
				uids = strings.Split(s1,"$$$")
			}
			listUserName := ""
			for _, uid := range uids{
				username, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("UserName", []byte(uid))
				if username != nil && username.Item != nil{
					s := string(username.Item.Value[:])
					listUserName = listUserName + s + "$"
				}
			}

			Cid.Listusername = listUserName
			client, _ := mp.Get("192.168.0.6", "18407").Get()
			defer client.BackToPool()
			str, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Cid-nMessId", []byte(cid))
			if str != nil && str.Item != nil{
				//s := string(str.Item.Value[:])
				s := strings.Split(string(str.Item.Value[:]), "$$$")

				lastMid := s[len(s)-1]
				messgae,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Message", []byte(lastMid))
				if  messgae != nil && messgae.Item != nil {
					str := strings.Split(string(messgae.Item.Value[:]), "$$$")

				if len(str)>=3 {Cid.LastMessage = str[0]
					Cid.LastedTime = str[2]
					Cid.LastMid = lastMid
					}
				}
			}
			listCid = append(listCid,&Cid)
		}
		return &pb.AllConversation{ListConversation:listCid},nil
	}
	return &pb.AllConversation{},nil
}

//tra ve tat ca cac tin nhan theo Cid
func (s *UserService) LoadMessOnCid(ctx context.Context, in *pb.Request) (*pb.AllMessages, error) {
	client, _ := mp.Get("192.168.0.6", "18407").Get()
	defer client.BackToPool()
	uid, _ := checkSessionKey(in.GetSessionkey())
	//count, _ := client.Client.(*bs.TStringBigSetKVServiceClient).GetTotalCount("Content")
	mess := []pb.Message{}
	var m pb.Message
	lstmess := []*pb.Message{}

	cidRequest := in.GetRequest()
	strLastMid := in.GetId()

	if uid != 0 {
		//Mess,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetSlice("Content",0, int32(count))
		dem := 0
		checkCidRequest, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsExisted("Cid-nMessId", []byte(cidRequest))
		if checkCidRequest.GetExisted() {
			fmt.Println(uid)
			str, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Cid-nMessId", []byte(cidRequest))
			if str != nil && str.Item != nil && str.Item.Value != nil{
				//s := string(str.Item.Value[:])
				s := strings.Split(string(str.Item.Value[:]), "$$$")
				if strLastMid != "" {
					var lastMid int
					for i, mid := range s {
						if mid == strLastMid {lastMid = i; break}
					}
					startMid := lastMid -20
					if startMid < 0 {
						startMid = 0
					}

					fmt.Println(startMid)
					fmt.Println(lastMid)

					for i:= startMid ; i<=lastMid ; i ++ {
						mid := s[i]
						if mid == "" {fmt.Println("111mid == ",mid );continue}
						if startMid == lastMid {break}
						m.Mid,_ = strconv.ParseUint(mid, 10, 64)

						s,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Message", []byte(mid))
						if  s != nil && s.Item != nil {
							str := strings.Split(string(s.Item.Value[:]), "$$$")
							if len(str) >=3{
							m.Content = str[0]
							m.FromName = str[1]
							m.CreatedTime = str[2]
							}
						//get Image, send..
							image,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Image", []byte(mid))
							if  image != nil && image.Item != nil {m.Image = image.Item.Value[:]}
							mess = append(mess, m)
							lstmess = append(lstmess, &mess[dem])
							dem ++
						}
					}
				}else{
					for _, mid := range s {
						//mid nao bi xoa thi mid == ""
						if mid == "" {fmt.Println("mid == ",mid );continue}
						m.Mid,_ = strconv.ParseUint(mid, 10, 64)
						s,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Message", []byte(mid))
						if  s != nil && s.Item != nil {
							str := strings.Split(string(s.Item.Value[:]), "$$$")

							if len(str) >=3{
								m.Content = str[0]
								m.FromName = str[1]
								m.CreatedTime = str[2]
							}
							mess = append(mess, m)
							lstmess = append(lstmess, &mess[dem])
							dem ++
						}
					}
				}
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

	client, _ := mp.Get("192.168.0.6", "18407").Get()
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
			//tim ToId
			username, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("UserName", []byte(key))
			if username != nil && username.Item != nil && username.Item.Value != nil{
				UserName := string(username.Item.Value[:])
				m.Username = UserName
				m.Uid = uint64(i)
				active, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Active", []byte(key))
				if active != nil && active.Item != nil && active.Item.Value != nil {
					Active := string(active.Item.Value[:])
					if Active =="1" {
						m.Active = true
					}else {m.Active =false}
					avatar, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Avatar", []byte(key))
					if avatar != nil && avatar.Item != nil{
						m.Avatar = avatar.Item.Value[:]
					}
					user = append(user, m)
					listuser =append(listuser, &user[dem])
					dem ++
				}}
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

	client, _ := mp.Get("192.168.0.6", "18407").Get()
	defer client.BackToPool()
	uid, _ := checkSessionKey(req.GetSessionkey())
	if uid != 0 {
		username := req.GetRequest()
		id, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("UserName_Id", []byte(username))
		if id != nil && id.Item != nil && id.Item.Value != nil{
			uid := string(id.Item.Value[:])
			return &pb.Response{Check: true, Response: uid}, nil
		}else{
			return &pb.Response{Check:false}, nil
		}
	} else{
		return &pb.Response{Check:false}, nil
	}
}
//luu tin nhan vao trong csdl
func saveMessage(mess pb.Message){
	client, _ := mp.Get("192.168.0.6", "18407").Get()
	defer client.BackToPool()
	//sinh mid
	idclient,_ := mpid.Get("192.168.0.6", "18405").Get()
	defer idclient.BackToPool()
	idclient.Client.(*idbs.TGeneratorClient).CreateGenerator("GenIdMessage")

	id := getValue("GenIdMessage")

	mid := strconv.Itoa(int(id))

	t := strconv.Itoa(int(time.Now().Unix()))
	mess.CreatedTime = t

	log.Println("[saveMessage]:Message time: ", t)

	s := mess.Content + "$$$" + mess.FromName + "$$$" + mess.CreatedTime
	
	client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Message", &bs.TItem{[]byte(mid),[]byte(s)})
	log.Println("[saveMessage]:Message Content: ",mess.Content," fromname: ",mess.Content,"  time: ",mess.CreatedTime )

	log.Println("[saveMessage]:Message Image")
	client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Image", &bs.TItem{[]byte(mid),mess.Image})

	str,_:= client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Cid-nMessId", []byte(mess.GetCid()))
	if str!= nil && str.Item != nil {
		s := string(str.Item.Value[:])
		s = s + "$$$" + mid
		client.Client.(*bs.TStringBigSetKVServiceClient).BsRemoveItem("Cid-nMessId", []byte(mess.GetCid()))
		client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Cid-nMessId", &bs.TItem{[]byte(mess.GetCid()),[]byte(s)})
		log.Println("[saveMessage]:Array Mid on Cid: ", s)
	} else {
		client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Cid-nMessId", &bs.TItem{[]byte(mess.GetCid()), []byte(mid)})
		log.Println("[saveMessage]:Fisrt Mid on Cid: ", mid)
	}
}

//check xem User co ton tai ko
func (s *UserService) CheckUser(ctx context.Context, in *pb.Request) (*pb.Response, error) {

	client, _ := mp.Get("192.168.0.6", "18407").Get()
	defer client.BackToPool()
	//truoc tien check xem no co online
	uid, _ := checkSessionKey(in.GetSessionkey())

	if uid != 0 {
		log.Println("[CheckUser]:uid: ", in.GetRequest())
		active, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Active", []byte(in.GetRequest()))
		if active != nil && active.Item != nil && active.Item.Value != nil {
			Active := string(active.Item.Value[:])
			if Active == "1" {
				log.Println("[CheckUser]:online: ")
				return &pb.Response{Response: "Online", Check: true}, nil
			} else {
				log.Println("[CheckUser]:offline: ")
				return &pb.Response{Response: "Offline", Check: false}, nil
			}
		}
	}
	return &pb.Response{Response: "UserName Not Exited", Check: false}, nil
}

//lang nghe tin nhan den
func listenToClient(stream pb.ChatgRPC_RouteChatServer, messages chan<- pb.Message, wg sync.WaitGroup, fromname string) {
	for {
		msg, err := stream.Recv()
		if msg != nil  {
			from_id, _ := checkSessionKey(msg.GetSessionkey())
			if from_id != 0 {
				if err == io.EOF {
					log.Println("[listenToClient]:err == io.EOF: ")
					return
				}
				msg.FromName = fromname
				saveMessage(*msg)
				log.Println("[listenToClient]:Save Message: ")
				messages <- *msg
			}
		}else {
			client, _ := mp.Get("192.168.0.6", "18407").Get()
			id, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("UserName_Id", []byte(fromname))
			if id!= nil && id.Item != nil{
				uid := string(id.Item.Value[:])
				client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Active", &bs.TItem{[]byte(uid), []byte("0")})
				log.Println("[listenToClient]:Active: ","No")
			}
			log.Println("[listenToClient]:Disconnect with Client : ",fromname)
			return
		}
	}
}
//gui tin cho cac user trong group
func broadcast(fromid string, msg pb.Message) {

	log.Println("[broadcast]:cid: ", msg.GetCid())
	uids := []string{}

	client, _ := mp.Get("192.168.0.6", "18407").Get()
	defer client.BackToPool()
	str,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Cid-nUid",[]byte(msg.GetCid()))
	if str != nil && str.Item != nil{
		strSplit := strings.Split(string(str.GetItem().GetValue()),"$$$")
		uids = strSplit
	}

	for _,uid := range uids {
		if uid != fromid {
			if clients[uid] != nil {
				//log.Println("[broadcast]:broadcast message for toid: ", msg)
				clients[uid].ch <- msg
			}
		}
	}
}

func (s *UserService)RouteChat(stream pb.ChatgRPC_RouteChatServer) error {

	var wg sync.WaitGroup
	wg.Add(1)
	mess,_ := stream.Recv()

	if mess != nil && mess.GetSessionkey() != ""{
		from_id,from_name := checkSessionKey(mess.GetSessionkey())

		if from_id !=0{
			clientMessages := make(chan pb.Message)
			go listenToClient(stream, clientMessages,wg, from_name)
			for {
				select {
				case messageFromClient := <-clientMessages:
					broadcast(strconv.Itoa(int(from_id)), messageFromClient)
					break
				case messageFromOthers := <-clients[strconv.Itoa(int(from_id))].ch:
					log.Println("[RouteChat]:send message to ",from_id)
					err := stream.Send(&messageFromOthers)
					if err !=nil{
						log.Println("[RouteChat]:Send message for toid: ")
					}
				}
			}
		}else {return nil}} else {return nil}
}
func (s *UserService) GetInfoUser(ctx context.Context, in *pb.Request) (*pb.User, error) {
	client, _ := mp.Get("192.168.0.6", "18407").Get()
	defer client.BackToPool()

	//truoc tien check xem no co online
	idsender,_ := checkSessionKey(in.GetSessionkey())
	var user pb.User
	if idsender != 0 {
		key := in.GetRequest()
	//tra ve ten user
		username, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("UserName", []byte(key))
		if username != nil && username.Item != nil{
			user.Username= string(username.Item.Value[:])}

	//tra ve avatar
		avatar, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Avatar", []byte(key))
		if avatar != nil && avatar.Item != nil{
			user.Avatar= avatar.Item.Value[:] }
	//tra ve Active
		active,_ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Active",[]byte(key))
		if active != nil && active.Item != nil{
			if string(active.Item.Value[:]) == "1" {
				user.Active = true
			} else {
				user.Active = false
			}
		}else {return &user, nil}
	}else {return &user, nil}
	return &user, nil
}

func a(){
	client, _ := mp.Get("192.168.0.6", "18407").Get()
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

func (s *UserService) DeleteMessage(ctx context.Context, in *pb.Request) (*pb.Response, error){

	client, _ := mp.Get("192.168.0.6", "18407").Get()
	defer client.BackToPool()
	uid, _ := checkSessionKey(in.GetSessionkey())
	//count, _ := client.Client.(*bs.TStringBigSetKVServiceClient).GetTotalCount("Content")

	cidRequest := in.GetRequest()
	strMid := in.GetId()

	if uid != 0 {
		str, _ := client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Cid-nMessId", []byte(cidRequest))
		if str != nil && str.Item != nil{
			//s := string(str.Item.Value[:])
			s := strings.Split(string(str.Item.Value[:]), "$$$")

			for i := 0; i < len(s); i++ {
				if  strMid == s[i] {
					s[i] = ""
					x := ""
					for i := 0; i < len(s); i++ {
						x = x + s[i] + "$$$"
					}
					str,_:= client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Cid-nMessId", []byte(cidRequest))
					if str != nil && str.Item != nil && str.Item.Value != nil{
						s := string(str.Item.Value[:])
						fmt.Println(s)
					}

					client.Client.(*bs.TStringBigSetKVServiceClient).BsRemoveItem("Cid-nMessId", []byte(cidRequest))
					client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("Cid-nMessId", &bs.TItem{[]byte(cidRequest),[]byte(x)})

					str,_= client.Client.(*bs.TStringBigSetKVServiceClient).BsGetItem("Cid-nMessId", []byte(cidRequest))
					if str != nil && str.Item != nil && str.Item.Value != nil{
						s := string(str.Item.Value[:])
						fmt.Println(s)}
					return &pb.Response{Check: true}, nil
				}
			}
		}
	}
	return &pb.Response{Check:false},nil
}
func (s *UserService) DeleteConversasion(ctx context.Context, in *pb.Request, ) (*pb.Response, error){
	return &pb.Response{},nil
}
func (s *UserService) UpdateInfo(ctx context.Context, in *pb.UserInfo,) (*pb.Response, error){

	client, _ := mp.Get("192.168.0.6", "18407").Get()
	defer client.BackToPool()
	uid, _ := checkSessionKey(in.GetSessionkey())
	//count, _ := client.Client.(*bs.TStringBigSetKVServiceClient).GetTotalCount("Content")
	if uid != 0 {
		key_name := strconv.FormatInt(uid,10)
		client.Client.(*bs.TStringBigSetKVServiceClient).BsRemoveItem("InfoUser",[]byte(key_name))
		str := in.GetInfouser().GetPassword() + " " + in.GetInfouser().GetEmail() + " " + in.GetInfouser().GetPhone()
		client.Client.(*bs.TStringBigSetKVServiceClient).BsPutItem("InfoUser", &bs.TItem{[]byte(key_name), []byte(str)})

		log.Println("[UpdateInfo]:UpdateInfo Success " + str)
		return &pb.Response{Check:true},nil
	}
	return &pb.Response{Check:false},nil
}
func main(){
	listen, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	pb.RegisterChatgRPCServer(s, &UserService{})
	fmt.Println("Listening on the 192.168.0.6:8000")

	if err := s.Serve(listen); err != nil {
		log.Fatal(err)
	}
	if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
		fmt.Println("Listening on the 192.168.0.6:8000")
	}
}

