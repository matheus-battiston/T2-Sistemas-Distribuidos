// Construido como parte da disciplina de Sistemas Distribuidos
// PUCRS - Escola Politecnica
// Professor: Fernando Dotti  (www.inf.pucrs.br/~fldotti)

/*
LANCAR N PROCESSOS EM SHELL's DIFERENTES, PARA CADA PROCESSO, O SEU PROPRIO ENDERECO EE O PRIMEIRO DA LISTA
go run chat.go 127.0.0.1:5001  127.0.0.1:6001    ...
go run chat.go 127.0.0.1:6001  127.0.0.1:5001    ...
go run chat.go ...  127.0.0.1:6001  127.0.0.1:5001
*/

package main

import (
	. "SD/URB"
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type envios struct {
	nome              string
	mensagensEnviadas int
}

func adicionaRecebido(quemMandou string, listaQuemMandou []envios) []envios {
	if !jaMandouMensagem(quemMandou, listaQuemMandou) {
		listaQuemMandou = append(listaQuemMandou, envios{
			nome:              quemMandou,
			mensagensEnviadas: 0})
	}

	for x := 0; x < len(listaQuemMandou); x++ {
		if listaQuemMandou[x].nome == quemMandou {
			listaQuemMandou[x].mensagensEnviadas++
		}
	}

	return listaQuemMandou

}

func jaMandouMensagem(nome string, listaQuemMandou []envios) bool {
	for i := 0; i < len(listaQuemMandou); i++ {
		if listaQuemMandou[i].nome == nome {
			return true
		}
	}
	return false
}

func enviarBroadcastsComFalha(numeroInt int, addresses []string, urb URB_Module) {
	var msg string

	for i := 0; i < 1000; i++ {
		numeroDaMsg := numeroInt + i
		numeroDaMsgString := strconv.Itoa(numeroDaMsg)
		msg = numeroDaMsgString + string("§") + string(addresses[0])
		if numeroDaMsg == 8000 {
			req := URB_Req_Message{
				Addresses: addresses[2:3],
				Message:   msg}
			urb.Req <- req
			<-urb.Ind
		} else {
			req := URB_Req_Message{
				Addresses: addresses[0:],
				Message:   msg}
			urb.Req <- req
		}
	}
}

func enviarBroadcastsSemFalha(numeroInt int, addresses []string, urb URB_Module) {
	var msg string

	for i := 0; i < 1000; i++ {
		numeroDaMsg := numeroInt + i
		numeroDaMsgString := strconv.Itoa(numeroDaMsg)
		msg = numeroDaMsgString + string("§") + string(addresses[0])
		req := URB_Req_Message{
			Addresses: addresses[0:],
			Message:   msg}
		urb.Req <- req
	}
}

func Write(fileName string, message []string) {

	f, err := os.Create(fileName)

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()
	for i := 0; i < len(message); i++ {
		_, err2 := f.WriteString(message[i] + "\n")

		if err2 != nil {
			log.Fatal(err2)
		}
	}
	fmt.Println("done")
}

func main() {

	if len(os.Args) < 2 {
		fmt.Println("Please specify at least one address:port!")
		fmt.Println("go run chat-BEBTest.go 127.0.0.1:5001  127.0.0.1:6001 127.0.0.1:7001")
		fmt.Println("go run chat-BEBTest.go 127.0.0.1:6001  127.0.0.1:5001 127.0.0.1:7001")
		fmt.Println("go run chat-BEBTest.go 127.0.0.1:7001  127.0.0.1:6001  127.0.0.1:5001")
		return
	}

	var contagemDeEnvios []envios
	var registro []string
	addresses := os.Args[1:]

	urb := URB_Module{
		Req: make(chan URB_Req_Message, 10000),
		Ind: make(chan URB_Ind_Message, 10000)}

	urb.Init(addresses[0], addresses[0:])

	// enviador de broadcasts
	go func() {

		var msg string
		scanner := bufio.NewScanner(os.Stdin)
		numero := strings.Split(addresses[0], ":")[1]
		numeroInt, err := strconv.Atoi(numero)
		_ = err

		if scanner.Scan() {
			msg = scanner.Text()
			if msg == "1" {
				enviarBroadcastsComFalha(numeroInt, addresses, urb)
			} else if msg == "2" {
				enviarBroadcastsSemFalha(numeroInt, addresses, urb)
			}
		}
	}()

	// receptor de broadcasts
	go func() {

		for {
			in := <-urb.Ind
			numero := strings.Split(addresses[0], ":")[1]
			numeroInt, err := strconv.Atoi(numero)
			_ = err
			message := strings.Split(in.Message, "§")
			contagemDeEnvios = adicionaRecebido(message[1], contagemDeEnvios)
			in.From = message[1]
			registro = append(registro, strings.Split(in.Message, "§")[0])
			in.Message = message[0]

			// imprime a mensagem recebida na tela
			fmt.Printf("          Message from %v: %v\n", in.From, in.Message)

			if len(registro) == 1 && in.From != addresses[0] {
				go enviarBroadcastsSemFalha(numeroInt, addresses, urb)
			}
			if len(registro) == len(addresses)*1000 {
				Write((addresses[0])+".txt", registro)
				fmt.Println(contagemDeEnvios, "ContagemEnvios")
				os.Exit(0)
			}
		}
	}()

	blq := make(chan int)
	<-blq
}
