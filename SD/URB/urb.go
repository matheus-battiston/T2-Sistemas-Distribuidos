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
	Ind          chan URB_Ind_Message
	Req          chan URB_Req_Message
	Quit         chan bool
	beb          BestEffortBroadcast_Module
	pending      []string
	delivered    []string
	addresses    []string
	dbg          bool
	listaComACKS []ACKS
}

func (module *URB_Module) foiEntrege(message string) bool {
	for i := 0; i < len(module.delivered); i++ {
		if module.delivered[i] == message {
			return true
		}
	}
	return false
}

func RemoveIndex(s []string, index int) []string {
	if len(s) == 1 {
		return []string{}
	}
	return append(s[:index], s[index+1:]...)
}

func (module *URB_Module) getIndex(message string) int {
	for i := 0; i < len(module.pending); i++ {
		if module.pending[i] == message {
			return i
		}
	}
	return -1
}

func (module *URB_Module) estaPendente(message string) bool {
	for i := 0; i < len(module.pending); i++ {
		if module.pending[i] == message {
			return true
		}
	}
	return false
}

func (module *URB_Module) adicionaAck(message URB_Ind_Message) {
	if !module.estaNosAcks(message.Message) {
		module.listaComACKS = append(module.listaComACKS, ACKS{
			Message:     message.Message,
			quantosAcks: 0,
		})
	}

	for i := 0; i < len(module.listaComACKS); i++ {
		if module.listaComACKS[i].Message == message.Message && naoDeuAckAinda(message.From, module.listaComACKS[i].quemDeuAck) {
			module.listaComACKS[i].quantosAcks++
			module.listaComACKS[i].quemDeuAck = append(module.listaComACKS[i].quemDeuAck, message.From)
		}
	}
}

func naoDeuAckAinda(quemEh string, listaDeAcks []string) bool {
	for i := 0; i < len(listaDeAcks); i++ {
		if listaDeAcks[i] == quemEh {
			return false
		}
	}
	return true
}

func (module *URB_Module) estaNosAcks(message string) bool {
	for i := 0; i < len(module.listaComACKS); i++ {
		if module.listaComACKS[i].Message == message {
			return true
		}
	}
	return false
}

func (module *URB_Module) naoTemAck(message string, acksDaMensagem string) bool {
	for i := 0; i < len(acksDaMensagem); i++ {
		if string(acksDaMensagem[i]) == string(message) {
			return true
		}
	}
	return false
}

func (module *URB_Module) outDbg(s string) {
	if module.dbg {
		fmt.Println(". . . . . . . . . [ URB msg : " + s + " ]")
	}
}

func (module *URB_Module) canDeliver(message string) bool {
	for i := 0; i < len(module.listaComACKS); i++ {
		if message == module.listaComACKS[i].Message {
			if module.listaComACKS[i].quantosAcks > ((len(module.addresses)) / 2) {
				return true
			}
		}
	}
	return false
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

	go func() {
		for {
			select {
			case y := <-module.Req:
				module.Broadcast(y)
			case y := <-module.beb.Ind:
				module.adicionaAck(URB_Ind_Message(y))
				if module.canDeliver(y.Message) && module.estaPendente(y.Message) && !module.foiEntrege(y.Message) {
					module.Deliver(URB_Ind_Message(y))
				} else if !module.estaPendente(y.Message) {
					module.pending = append(module.pending, y.Message)
					msg := URB_Req_Message{
						Addresses: module.addresses,
						Message:   y.Message,
					}
					module.Broadcast(msg)
				}
			}

		}
	}()
}

func (module *URB_Module) quit() {
	close(module.Ind)

}

func (module *URB_Module) Broadcast(message URB_Req_Message) {

	if !module.estaPendente(message.Message) {
		module.pending = append(module.pending, message.Message)
	}
	req := BestEffortBroadcast_Req_Message{
		Addresses: module.addresses,
		Message:   message.Message}
	module.beb.Req <- req
}

func (module *URB_Module) Deliver(message URB_Ind_Message) {

	module.delivered = append(module.delivered, message.Message)
	index := module.getIndex(message.Message)
	module.pending = RemoveIndex(module.pending, index)
	// fmt.Println("Received '" + message.Message + "' from " + message.From)
	module.Ind <- message
	// fmt.Println("# End BEB Received")
}
