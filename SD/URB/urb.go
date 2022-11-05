/*
Construido como parte da disciplina: Sistemas Distribuidos - PUCRS - Escola Politecnica
Professor: Fernando Dotti  (https://fldotti.github.io/)
Modulo representando Berst Effort Broadcast tal como definido em:

	Introduction to Reliable and Secure Distributed Programming
	Christian Cachin, Rachid Gerraoui, Luis Rodrigues

* Semestre 2018/2 - Primeira versao.  Estudantes:  Andre Antonitsch e Rafael Copstein
Para uso vide ao final do arquivo, ou aplicacao chat.go que usa este
*/
package Urb

import (
	. "SD/BEB"
	"fmt"
)

type URB_Req_Message struct {
	Addresses []string
	Message   string
	TimeStamp int
}

type URB_Ind_Message struct {
	From    string
	Message string
}

type ACKS struct {
	Message     string
	quemDeuAck  []string
	quantosAcks int
}
type URB_Module struct {
	Ind              chan URB_Ind_Message
	Req              chan URB_Req_Message
	Quit             chan bool
	beb              BestEffortBroadcast_Module
	addresses        []string
	dbg              bool
	listaDeMensagens []Local
	final            []Mensagem
	clock            int
	deliver          []Mensagem
}

type Local struct {
	Message string
	Tempo   []int
}

type Mensagem struct {
	Message string
	Tempo   int
}

func (module *URB_Module) adicionaTimeStamp(message Mensagem) {
	for i := 0; i < len(module.listaDeMensagens); i++ {
		if message.Message == module.listaDeMensagens[i].Message {
			module.listaDeMensagens[i].Tempo = append(module.listaDeMensagens[i].Tempo, message.Tempo)
		}
	}
}
func (module *URB_Module) RemoveIndex(index int) []Local {
	return append(module.listaDeMensagens[:index], module.listaDeMensagens[index+1:]...)
}

func (module *URB_Module) jaRecebeu(message Mensagem) bool {
	for i := 0; i < len(module.listaDeMensagens); i++ {
		if message.Message == module.listaDeMensagens[i].Message {
			return true
		}
	}

	return false
}

func (module *URB_Module) adicionaMensagemLocal(message Mensagem) {
	var timeStampLocal = []int{module.clock}
	var mensagem = Local{
		Message: message.Message,
		Tempo:   timeStampLocal,
	}
	module.listaDeMensagens = append(module.listaDeMensagens, mensagem)
}

func (module *URB_Module) receive(message Mensagem) {
	if module.jaRecebeu(message) {
		fmt.Println(message.Message, "JA RECEBEU")
		module.adicionaTimeStamp(message)
	} else {
		fmt.Println(message.Message, "NAO RECEBEU")
		module.clock = module.clock + 1
		module.adicionaMensagemLocal(message)
		module.adicionaTimeStamp(message)
		module.Broadcast(URB_Req_Message{
			Addresses: module.addresses,
			Message:   message.Message,
			TimeStamp: module.clock,
		})
	}

	if module.checarSeRecebeuDeTodos(message) {
		module.jaRecebeuDeTodos(message)
	}
}

func (module *URB_Module) checarSeRecebeuDeTodos(message Mensagem) bool {
	for i := 0; i < len(module.listaDeMensagens); i++ {
		if message.Message == module.listaDeMensagens[i].Message {
			tamanhoTS := len(module.listaDeMensagens[i].Tempo) - 1
			numeroDeClients := len(module.addresses)
			fmt.Println(tamanhoTS, numeroDeClients)
			return tamanhoTS == numeroDeClients
		}
	}
	return false
}

func (module *URB_Module) jaRecebeuDeTodos(message Mensagem) {
	var msgLocal = module.getMensagemDaLista(message)
	var maxTS = getMaxTS(msgLocal)
	module.final = module.addFinalOrdenado(Mensagem{
		Message: message.Message,
		Tempo:   maxTS,
	})
	var index = module.getIndex(message)

	module.listaDeMensagens = module.RemoveIndex(index)

	module.clock = getMaxClock(module.clock, maxTS)
	module.tryDeliver()
}

func (module *URB_Module) addFinalOrdenado(message Mensagem) []Mensagem {
	var novoModuloFinal []Mensagem
	var tamanhoFinal = len(module.final)
	if tamanhoFinal == 0 {
		novoModuloFinal = []Mensagem{message}
	} else {
		for i := 0; i < tamanhoFinal; i++ {
			if message.Tempo < module.final[i].Tempo {
				novoModuloFinal = append(module.final[:i+1], module.final[i:]...)
				novoModuloFinal[i] = message
			}
		}
	}

	return novoModuloFinal
}

func (module *URB_Module) getIndex(message Mensagem) int {
	for i := 0; i < len(module.listaDeMensagens); i++ {
		if message.Message == module.listaDeMensagens[i].Message {
			return i
		}
	}
	return -1
}

func (module *URB_Module) getIndexFinal(message Mensagem) int {
	for i := 0; i < len(module.final); i++ {
		if message.Message == module.final[i].Message {
			return i
		}
	}
	return -1
}

func (module *URB_Module) RemoveIndexFinal(index int) []Mensagem {
	return append(module.final[:index], module.final[index+1:]...)
}

func (module *URB_Module) tryDeliver() {
	fmt.Println("TRY DELIVER")
	for i := 0; i < len(module.final); i++ {
		for j := 0; j < len(module.listaDeMensagens); j++ {
			var max = getMaxTS(module.listaDeMensagens[i])
			if max < module.final[i].Tempo {
				return
			}
		}
		module.deliver = append(module.deliver, module.final[i])
		module.Deliver(URB_Ind_Message{
			Message: module.final[i].Message,
		})
		module.final = module.RemoveIndexFinal(module.getIndexFinal(module.final[i]))

	}
}

func (module *URB_Module) getMensagemDaLista(message Mensagem) Local {
	for i := 0; i < len(module.listaDeMensagens); i++ {
		if message.Message == module.listaDeMensagens[i].Message {
			return module.listaDeMensagens[i]
		}
	}
	return Local{}
}

func getMaxTS(message Local) int {
	var timeStamp = 0

	for i := 0; i < len(message.Tempo); i++ {
		if message.Tempo[i] > timeStamp {
			timeStamp = message.Tempo[i]
		}
	}

	return timeStamp
}

func getMaxClock(clock int, timeStamp int) int {
	if clock > timeStamp {
		return clock
	} else if timeStamp > clock {
		return timeStamp
	}

	return clock
}

func (module *URB_Module) outDbg(s string) {
	if module.dbg {
		fmt.Println(". . . . . . . . . [ URB msg : " + s + " ]")
	}
}

func (module *URB_Module) Init(address string, addresses []string) {
	module.InitD(address, true, addresses)
}

func (module *URB_Module) InitD(address string, _dbg bool, addresses []string) {

	module.addresses = addresses
	module.dbg = _dbg
	module.outDbg("Init URB!")
	module.beb = BestEffortBroadcast_Module{
		Req: make(chan BestEffortBroadcast_Req_Message, 10000),
		Ind: make(chan BestEffortBroadcast_Ind_Message, 10000)}
	module.beb.Init(address)

	module.Start()
}

func (module *URB_Module) Start() {

	module.clock = 0
	go func() {
		for {
			select {
			case y := <-module.Req:
				module.Broadcast(y)
				module.adicionaMensagemLocal(Mensagem{
					Message: y.Message,
					Tempo:   y.TimeStamp,
				})
			case y := <-module.beb.Ind:
				module.receive(Mensagem{
					Message: y.Message,
					Tempo:   y.TimeStamp,
				})
			}

		}
	}()
}

func (module *URB_Module) quit() {
	close(module.Ind)

}

func (module *URB_Module) Broadcast(message URB_Req_Message) {

	req := BestEffortBroadcast_Req_Message{
		Addresses: module.addresses,
		Message:   message.Message,
		TimeStamp: message.TimeStamp}
	module.beb.Req <- req
}

func (module *URB_Module) Deliver(message URB_Ind_Message) {

	fmt.Println("DELIVERED")
	// fmt.Println("Received '" + message.Message + "' from " + message.From)
	module.Ind <- message
	// fmt.Println("# End BEB Received")
}
