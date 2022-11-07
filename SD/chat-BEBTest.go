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

func enviarBroadcastsSemFalha(numeroInt int, addresses []string, urb URB_Module) {
	var msg string

	go func() {
		for i := 0; i < 50; i++ {
			numeroDaMsg := numeroInt + i
			numeroDaMsgString := strconv.Itoa(numeroDaMsg)
			msg = numeroDaMsgString + string("§") + string(addresses[0]) + "/-1"
			req := URB_Req_Message{
				Addresses: addresses[1:],
				Message:   msg}
			urb.Req <- req
		}
	}()

	go func() {
		for i := 0; i < 50; i++ {
			numeroDaMsg := numeroInt + i + 5000
			numeroDaMsgString := strconv.Itoa(numeroDaMsg)
			msg = numeroDaMsgString + string("§") + string(addresses[0]) + "/-1"
			req := URB_Req_Message{
				Addresses: addresses[1:],
				Message:   msg}
			urb.Req <- req
		}
	}()
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
		Req: make(chan URB_Req_Message),
		Ind: make(chan URB_Ind_Message, 10000)}

	urb.Init(addresses[0], addresses[1:])

	// enviador de broadcasts
	go func() {

		var msg string
		scanner := bufio.NewScanner(os.Stdin)
		numero := strings.Split(addresses[0], ":")[1]
		numeroInt, err := strconv.Atoi(numero)
		_ = err

		if scanner.Scan() {
			msg = scanner.Text()
			if msg == "2" {
				enviarBroadcastsSemFalha(numeroInt, addresses, urb)
			}
		}
	}()

	// receptor de broadcasts
	go func() {

		for {
			in := <-urb.Ind
			message := strings.Split(in.Message, "§")
			contagemDeEnvios = adicionaRecebido(message[1], contagemDeEnvios)
			in.Tempo = message[1]
			registro = append(registro, strings.Split(in.Message, "§")[0])
			in.Message = message[0]

			// imprime a mensagem recebida na tela
			// fmt.Printf("          Message from %v: %v\n", in.Tempo, in.Message)

			if len(registro) == 100 {
				Write((addresses[0])+".txt", registro)
				fmt.Println(registro)
				// fmt.Println(contagemDeEnvios, "ContagemEnvios")
			}
		}
	}()

	blq := make(chan int)
	<-blq
}
