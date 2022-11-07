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
	"math"
	"strconv"
	"strings"
)

type URB_Req_Message struct {
	Addresses []string
	Message   string
	TimeStamp int
}

type URB_Ind_Message struct {
	Tempo   string
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

var CONST_TS_PADRAO = -1

// Função para adicionar um TimeStamp a uma mensagem que ja foi adicioanda
func (module *URB_Module) adicionaTimeStamp(message Mensagem) {
	for i := 0; i < len(module.listaDeMensagens); i++ {
		if message.Message == module.listaDeMensagens[i].Message {
			module.listaDeMensagens[i].Tempo = append(module.listaDeMensagens[i].Tempo, message.Tempo)
		}
	}
}

// Função para remover uma mensagem da lista de mensagens local dado um indice
func (module *URB_Module) RemoveIndex(index int) []Local {
	return append(module.listaDeMensagens[:index], module.listaDeMensagens[index+1:]...)
}

// Função para identificar se uma determinada mensagem ja foi recebida antes ou nao
func (module *URB_Module) jaRecebeu(message Mensagem) bool {
	for i := 0; i < len(module.listaDeMensagens); i++ {
		if message.Message == module.listaDeMensagens[i].Message {
			return true
		}
	}
	return false
}

// Função para adicionar a lista de mensagens local uma mensagem
func (module *URB_Module) adicionaMensagemLocal(message Mensagem) {
	var timeStampLocal = []int{module.clock}
	var mensagem = Local{
		Message: message.Message,
		Tempo:   timeStampLocal,
	}
	module.listaDeMensagens = append(module.listaDeMensagens, mensagem)
}

// Função para tratar uma mensagem que foi recebida, identificando o que deve ser adicionado, ela toda ou apenas o timestamp
func (module *URB_Module) tratamentoMensagem(mensagemOrignal Mensagem, mensagemASerAdicionada Mensagem) {
	if !module.jaRecebeu(mensagemOrignal) {
		module.adicionaMensagemLocal(mensagemASerAdicionada)
	} else {
		module.adicionaTimeStamp(mensagemASerAdicionada)
	}
}

// Função para identificar se ja foi recebido o timestamp de todos os outros processos para uma determinada mensagem
func (module *URB_Module) checarSeRecebeuDeTodos(message Mensagem) bool {
	for i := 0; i < len(module.listaDeMensagens); i++ {
		if message.Message == module.listaDeMensagens[i].Message {
			tamanhoTS := len(module.listaDeMensagens[i].Tempo)
			numeroDeClients := len(module.addresses) + 1
			return tamanhoTS == numeroDeClients
		}
	}
	return false
}

// Função utilizada quando ja foi recebido o timestamp de todos os processos. Irá adicionar a lista de Final e tentara fazer o delivery
func (module *URB_Module) jaRecebeuDeTodos(message Mensagem) {
	var msgLocal = module.getMensagemDaLista(message)
	var maxTS = getMaxTS(msgLocal)
	module.addFinal(Mensagem{
		Message: message.Message,
		Tempo:   maxTS,
	})
	var index = module.getIndex(message)

	module.listaDeMensagens = module.RemoveIndex(index)

	module.clock = getMaxClock(module.clock, maxTS)
	module.tryDeliver()
}

// Adiciona uma mensagem a lista Final
func (module *URB_Module) addFinal(message Mensagem) {
	module.final = append(module.final, message)
}

// Função para retornar o indice de uma determinada mensagem na lista de mensagens local
func (module *URB_Module) getIndex(message Mensagem) int {
	for i := 0; i < len(module.listaDeMensagens); i++ {
		if message.Message == module.listaDeMensagens[i].Message {
			return i
		}
	}
	return -1
}

// Função para retornar o indice de uma determinada mensagem na lista de mensagem Final
func (module *URB_Module) getIndexFinal(message Mensagem) int {
	for i := 0; i < len(module.final); i++ {
		if message.Message == module.final[i].Message {
			return i
		}
	}
	return -1
}

// Função para remover uma mensagem da lista Final dado um indice
func (module *URB_Module) RemoveIndexFinal(index int) {
	module.final = append(module.final[:index], module.final[index+1:]...)
}

// Função para retornar o indice da mensagem com menor timestamp ta lista Final
func (module *URB_Module) getMin() int {
	var index = -1
	var min = math.Inf(1)
	for i := 0; i < len(module.final); i++ {
		if float64(module.final[i].Tempo) < min {
			min = float64(module.final[i].Tempo)
			index = i
		}
	}

	return index
}

// Função para checar se existe uma mensagem com timeStamp menor que o dado esperando receber o timestamp de outros processos
func (module *URB_Module) temMenorEsperando(tempo int) bool {
	for i := 0; i < len(module.listaDeMensagens); i++ {
		var max = getMaxTS(module.listaDeMensagens[i])
		if max < tempo {
			return true
		}
	}

	return false
}

// Função para detectar se existe algum empate em relação a decisão do timestamp
func (module *URB_Module) temEmpate(tempo int) bool {
	for i := 0; i < len(module.final); i++ {
		var tempoPesquisa = module.final[i].Tempo
		if tempo == tempoPesquisa {
			return true
		}
	}
	return false
}

// // Função para atualizar a lista final, dado uma lista de indices que devem ser removidos
// func (module *URB_Module) atualizaFinal(indexRemove []int) []Mensagem {
// 	var novoFinal = []Mensagem{}
// 	var procuraIndex = 0
// 	for i := 0; i < len(module.final); i++ {
// 		if i != indexRemove[procuraIndex] {
// 			novoFinal = append(novoFinal, module.final[i])

// 		} else {
// 			procuraIndex = procuraIndex + 1
// 		}
// 	}

// 	return novoFinal
// }

// Função que dado uma mensagem retorna o que ja está armazenado localmente
func (module *URB_Module) getMensagemDaLista(message Mensagem) Local {
	for i := 0; i < len(module.listaDeMensagens); i++ {
		if message.Message == module.listaDeMensagens[i].Message {
			return module.listaDeMensagens[i]
		}
	}
	return Local{}
}

// Função para retornar o maior TimeStamp da lista de mensagens local
func getMaxTS(message Local) int {
	var timeStamp = 0
	for i := 0; i < len(message.Tempo); i++ {
		if message.Tempo[i] > timeStamp {
			timeStamp = message.Tempo[i]
		}
	}

	return timeStamp
}

// Função para retornar o maior valor entre dois inteiros
func getMaxClock(clock int, timeStamp int) int {
	if clock > timeStamp {
		return clock
	} else if timeStamp > clock {
		return timeStamp
	}

	return clock
}

// Função para lidar com o recebimento de mensagens
func (module *URB_Module) receive(message Mensagem) {

	if message.Tempo == CONST_TS_PADRAO {
		module.clock = module.clock + 1
		var msg = Mensagem{
			Message: message.Message,
			Tempo:   module.clock,
		}
		module.tratamentoMensagem(message, msg)

		module.Broadcast(URB_Req_Message{
			Addresses: module.addresses,
			Message:   message.Message + "/" + strconv.Itoa(module.clock),
		})

	} else {
		module.tratamentoMensagem(message, message)
	}

	if module.checarSeRecebeuDeTodos(message) {
		module.jaRecebeuDeTodos(message)
	}
}

// Função que fara as tentativas de entrega das mensagens
func (module *URB_Module) tryDeliver() {
	for {
		if len(module.final) > 0 {
			var indexMin = module.getMin()
			fmt.Println(module.temEmpate(indexMin))
			if module.temMenorEsperando(indexMin) {
				return
			} else {
				module.Deliver(URB_Ind_Message{
					Message: module.final[indexMin].Message,
					Tempo:   strconv.Itoa(module.final[indexMin].Tempo),
				})
				module.RemoveIndexFinal(indexMin)
			}
		} else {
			return

		}
	}
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
				go module.Broadcast(y)
			case y := <-module.beb.Ind:
				var conteudoMensagem = strings.Split(y.Message, "/")[0]
				var tempo, e = strconv.Atoi(strings.Split(y.Message, "/")[1])
				_ = e
				module.receive(Mensagem{
					Message: conteudoMensagem,
					Tempo:   tempo,
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
		Message:   message.Message}
	module.beb.Req <- req
}

func (module *URB_Module) Deliver(message URB_Ind_Message) {

	if message.Tempo == "100" {
		fmt.Println(module.final, module.listaDeMensagens)
	}
	fmt.Println("DELIVERED", message)
	module.Ind <- message
	// fmt.Println("# End BEB Received")
}
