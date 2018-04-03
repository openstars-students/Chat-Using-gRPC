package main

import (
	"fmt"
	"log"
	pb "example/Chat-Using-gRPC/Chat_gRPC"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"bufio"
	"os"
	"strings"
	"github.com/golang/protobuf/ptypes"
	"time"
	"strconv"
	"os/signal"
	"syscall"

)

const (
	address = "127.0.0.1:8000"
)

var sessionkey string

func chat(c pb.ChatgRPCClient, idreceiver string, idconversation string) bool{

	reader := bufio.NewReader(os.Stdin)
	stream,_ := c.RouteChat(context.Background())
	stream.Send(&pb.Message{Sessionkey:sessionkey, Content:"hello", ToUid: idreceiver, Cid:idconversation})

	mailBox := make(chan pb.Message, 100)
	go receiveMessages(stream, mailBox)
	sendQ := make(chan pb.Message, 100)
	go listenToClient(sendQ, reader, idreceiver,idconversation )

	for {
		select {
		case toSend := <-sendQ:
			switch msg := toSend.Content; msg{
			case "!members":
				log.Println("[Main]: I'm in !members.")
			case "!leave":
				log.Println("[Main]: I'm in !leave.")
			case "!exit":
				log.Println("[Main]: I'm in !exit.")
				return true
			default:
				//log.Println("[Main]: Sending the message.")
				stream.Send(&toSend)}
		case received := <-mailBox:
			fmt.Printf("%s",received.FromName)
			fmt.Printf(" > %s",received.Content)
			fmt.Println()
		}
	}
}

//moi lan nhap la 1 lan su dung RouteChat()
func chatRieng(c pb.ChatgRPCClient, idreceiver string, cid string){
	fmt.Println("Chat Start:")
	for  {
		var a pb.Message
		reader := bufio.NewReader(os.Stdin)
		mess, _ := reader.ReadString('\n')
		mess = strings.TrimSpace(mess)
		if mess =="exit" {return}
		a.Content = mess
		a.Sessionkey = sessionkey

		a.ToUid = idreceiver
		t := strconv.Itoa(int(time.Now().UTC().UnixNano()))
		a.CreatedTime = t
		//fmt.Println(t)
		a.Cid = cid

		stream,_ := c.RouteChat(context.Background())
		err := stream.Send(&a)
		if err != nil {
			log.Fatal("client Chat send chat error: ", err)
		}

		reader.Reset(reader)
	}
}
//kiem tra xem co ai dang online khong

func singeChat(c pb.ChatgRPCClient){

	fmt.Print("nhap ten nguoi: ")
	reader := bufio.NewReader(os.Stdin)
	receiver, _ := reader.ReadString('\n')
	receiver = strings.TrimSpace(receiver)

	var req pb.Request
	req.Request =receiver
	req.Sessionkey = sessionkey
	//lay id cua nguoi nhan
	to_id,_ := c.GetId(context.Background(),&req)

	if to_id.GetCheck() {
		var req_cid pb.Request
		req_cid.Request = to_id.GetId()
		req_cid.Sessionkey = sessionkey

		//lay idconversation cua ban va nguoi nhan
		cid, _ := c.CreateConversation(context.Background(), &req_cid)

		//cid := "1"
		//fmt.Println("cid : ", cid)
		var username pb.Request
		username.Request = to_id.GetId()
		username.Sessionkey = sessionkey
		check, _ := c.CheckUser(context.Background(), &username)
		//check.Check de xem ho co online ko
		if check.GetCheck() {
			chat(c, to_id.GetId(), cid.GetId())
		} else {
			fmt.Println("hien tai khong online, hay de lai loi nhan")
			chatRieng(c,to_id.GetId(), cid.GetId())
		}
	}else {fmt.Println("ten khong ton tai")}
}

func runLoadWaittingMess(c pb.ChatgRPCClient){
	//fmt.Println("tin nhan chua duoc doc")
	var request pb.Request
	request.Sessionkey = sessionkey
	lstmess,err:=c.LoadWaittingMess(context.Background(),&request)

	if err != nil {
		//log.Fatal("ListUser stream error: ", err)
		return
	}
	for i:=0;i<len(lstmess.GetWaittingmess());i++{
		fmt.Println(lstmess.GetWaittingmess()[i].GetFromName()," >> ",lstmess.GetWaittingmess()[i].GetContent())
	}
}

func runLoadAllMessOnCid(c pb.ChatgRPCClient){

	fmt.Print("Nhap Cid: ")
	cid := bufio.NewReader(os.Stdin)
	Cid,_ := cid.ReadString('\n')
	Cid = strings.TrimSpace(Cid)
	var request pb.Request
	request.Sessionkey = sessionkey
	request.Request = Cid

	lstmess,err:=c.LoadAllMessOnCid(context.Background(),&request)

	if err != nil {
		log.Fatal("ListUser stream error: ", err)
		return
	}
	for i:=0;i<len(lstmess.GetAllmess());i++{
		fmt.Println(lstmess.GetAllmess()[i].GetFromName()," >> ",lstmess.GetAllmess()[i].GetContent())
	}
}
func runLogout(c pb.ChatgRPCClient) bool{
	//truyen vao sessionkey
	var req pb.Request
	req.Sessionkey = sessionkey
	logout,_ := c.Logout(context.Background(),&req)
	fmt.Println(logout.GetResponse())
	return false
}
func runGetAllConversation(c pb.ChatgRPCClient){

	var request pb.Request
	request.Sessionkey = sessionkey

	lstCid,err:=c.GetAllConversation(context.Background(),&request)

	if err != nil {
		log.Fatal("GetAllConversation error: ", err)
		return
	}
	fmt.Println(lstCid.GetListConversation())
	/*
	for i:=0;i<len(lstCid.GetListConversation());i++{

		fmt.Println("Cid: ",lstCid.GetListConversation()[i].GetCid())
	}
	*/
}
func runLogin(c pb.ChatgRPCClient) {

	var UserName pb.UserLogin
	for {
		fmt.Print("Nhap UserName: ")
		user := bufio.NewReader(os.Stdin)
		UserName.Username, _ = user.ReadString('\n')
		UserName.Username = strings.TrimSpace(UserName.Username)

		fmt.Print("Nhap Password: ")
		pass := bufio.NewReader(os.Stdin)
		UserName.Password, _ = pass.ReadString('\n')
		UserName.Password = strings.TrimSpace(UserName.Password)

		login, _ := c.Login(context.Background(), &UserName)

		check := login.GetCheck()
		if check == false {
			fmt.Println("UserName hoac password nhap khong dung")
			fmt.Println(login.Response)
		} else {
			//change session first
			sessionkey = login.GetResponse()
			fmt.Println("sessionkey: ",sessionkey)
			//join chat
			runLoadWaittingMess(c)
			show := true
			for show{
				TopMenuChat()
				reader := bufio.NewReader(os.Stdin)
				keyboad, _ := reader.ReadString('\n')
				keyboad = strings.TrimSpace(keyboad)
				switch keyboad {
				case "1":
					singeChat(c)
				case "2":
					createGroup(c)
				case "3":
					joinGroup(c)
				case "4":
					runLogout(c)
					return
				case "5":
					runGetListUser(c)
				case "7":
					runLoadAllMessOnCid(c)
				case "8":
					runAddUidToConversation(c)
				case "11":
					runGetAllConversation(c)

				}
			}
		}
	}
}

func controlExit() bool {
	for {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigs
		fmt.Println()
		fmt.Println("sign: ", sig)
		return false
	}
}

func runRegister(c pb.ChatgRPCClient){

	var InfoUser pb.User
	fmt.Print("Nhap UserName: ")
	user := bufio.NewReader(os.Stdin)
	InfoUser.Username ,_  = user.ReadString('\n')
	InfoUser.Username = strings.TrimSpace(InfoUser.Username)

	fmt.Print("Nhap Password: ")
	password := bufio.NewReader(os.Stdin)
	InfoUser.Password ,_  = password.ReadString('\n')
	InfoUser.Password = strings.TrimSpace(InfoUser.Password)

	fmt.Print("Nhap Email: ")
	email := bufio.NewReader(os.Stdin)
	InfoUser.Email ,_  = email.ReadString('\n')
	InfoUser.Email = strings.TrimSpace(InfoUser.Email)

	fmt.Print("Nhap Phone Number: ")
	phone := bufio.NewReader(os.Stdin)
	InfoUser.Phone ,_  = phone.ReadString('\n')
	InfoUser.Phone = strings.TrimSpace(InfoUser.Phone)

	Timestamp, _ := ptypes.TimestampProto(time.Now())
	i := int(Timestamp.Seconds)
	InfoUser.CreatedTime = strconv.Itoa(i)

	fmt.Println("time: ",InfoUser.CreatedTime )
	//InfoUser.Active = true
	mes,_ := c.Register(context.Background(), &InfoUser)

	fmt.Println("Register: ",mes.GetResponse())
}

func listenToClient(sendQ chan pb.Message, reader *bufio.Reader, idreceiver string, idconversation string) {
	for {
		mess, _ := reader.ReadString('\n')
		mess = strings.TrimSpace(mess)

		t := strconv.Itoa(int(time.Now().UTC().UnixNano()))
		//fmt.Println("createdtime: ", t)
		sendQ <- pb.Message{Sessionkey:sessionkey, Content:mess, CreatedTime:t, ToUid:idreceiver, Cid:idconversation}
	}
}

func receiveMessages(stream pb.ChatgRPC_RouteChatClient, mailbox chan pb.Message) {
	for {
		msg, err := stream.Recv()
		if err !=nil{
			fmt.Println(err)
			return
		}
		mailbox <- *msg
	}
}
func createGroup(c pb.ChatgRPCClient){

	fmt.Printf("Nhap id cac ban be: ")
	reader := bufio.NewReader(os.Stdin)
	grouper, _ := reader.ReadString('\n')
	grouper = strings.TrimSpace(grouper)

	var req_cid pb.Request
	req_cid.Request = grouper
	req_cid.Sessionkey = sessionkey
	gid, _ := c.CreateConversation(context.Background(), &req_cid)
	fmt.Println("gid: ", gid)
}
func joinGroup(c pb.ChatgRPCClient){

	fmt.Printf("Nhap gid: ")
	reader := bufio.NewReader(os.Stdin)
	gid, _ := reader.ReadString('\n')
	gid = strings.TrimSpace(gid)

	chat(c, "", gid)
}
func runGetListUser(c pb.ChatgRPCClient){

	var request pb.Request
	request.Sessionkey = sessionkey
	lstUser,err:=c.GetListUser(context.Background(),&request)

	if err != nil {
		//log.Fatal("ListUser stream error: ", err)
		return
	}
	for i:=0;i<len(lstUser.GetAlluser());i++{
		fmt.Println("Id: ",lstUser.GetAlluser()[i].GetUid()," >> ",lstUser.GetAlluser()[i].GetUsername(), " Active: ", lstUser.GetAlluser()[i].GetActive())
	}

}
func runAddUidToConversation(c pb.ChatgRPCClient){
	var req  pb.ConversationDetail

	fmt.Printf("Nhap uid: ")
	reader := bufio.NewReader(os.Stdin)
	uid, _ := reader.ReadString('\n')
	uid = strings.TrimSpace(uid)

	s_uid := strings.Split(uid, " ")
	fmt.Printf("Nhap cid: ")
	reader = bufio.NewReader(os.Stdin)
	cid, _ := reader.ReadString('\n')
	cid = strings.TrimSpace(cid)

	req.Cid = cid
	req.Uid = s_uid
	req.Sessionkey = sessionkey

	check, _ := c.AddUidToConversation(context.Background(),&req)
	if check.GetCheck(){
		fmt.Println("add success")
	}else{
		fmt.Println("dont success")
	}

}
func main() {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatal("dail error:", err)
	}
	defer conn.Close()
	//tao 1 c
	c := pb.NewChatgRPCClient(conn)
	//login(c,"Huyen", "432")

	//Register(c)
	//chat(c)
	show := true
	for show {
		TopMenuText()
		reader := bufio.NewReader(os.Stdin)
		keyboad, _ := reader.ReadString('\n')
		keyboad = strings.TrimSpace(keyboad)
		//test session by run ^C = disconect to server

		switch keyboad {
		case "1":
			runLogin(c)
		case "2":
			runRegister(c)
		}
	}
}
//dang sua o func loadmess
