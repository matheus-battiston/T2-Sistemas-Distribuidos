/*
Construido como parte da disciplina: Sistemas Distribuidos - PUCRS - Escola Politecnica
Professor: Fernando Dotti  (https://fldotti.github.io/)
Modulo representando Berst Effort Broadcast tal como definido em:

	Introduction to Reliable and Secure Distributed Programming
	Christian Cachin, Rachid Gerraoui, Luis Rodrigues

* Semestre 2018/2 - Primeira versao.  Estudantes:  Andre Antonitsch e Rafael Copstein
Para uso vide ao final do arquivo, ou aplicacao chat.go que usa este
*/
package skeen

import (
	. "SD/BEB"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type SKEEN_Req_Message struct {
	Addresses []string
	Message   string
	TimeStamp int
}

type SKEEN_Ind_Message struct {
	Tempo   string
	Message string
}

type ACKS struct {
	Message     string
	quemDeuAck  []string
	quantosAcks int
}
type SKEEN_Module struct {
	Ind              chan SKEEN_Ind_Message
	Req              chan SKEEN_Req_Message
	Quit             chan bool
	beb              BestEffortBroadcast_Module
	addresses        []string
	dbg              bool
	listaDeMensagens []Local
	final            []Mensagem
	clock            int
	deliver          []Mensagem
	ID               int
	PartitionID      int
}

type Local struct {
	Message     string
	Tempo       []int
	PartitionID []int
}

type Mensagem struct {
	Message     string
	Tempo       int
	PartitionID int
}

type mensagemAux struct {
	Message string
}

var CONST_TS_PADRAO = -1

func (module *SKEEN_Module) procuraEmpate(indiceDoEscolhido int) []int {
	var indicesEmpatados = []int{}
	for i := 0; i < len(module.final); i++ {
		if i != indiceDoEscolhido && module.final[i].Tempo == module.final[indiceDoEscolhido].Tempo {
			indicesEmpatados = append(indicesEmpatados, i)
		}
	}

	return indicesEmpatados
}

func (module *SKEEN_Module) decideEmpate(indicesEmpate []int) []int {
	var novaListaInteiros = []int{}
	var listaIndices = indicesEmpate

	for {
		var maior = -1
		var indiceMaior = -1
		for i := 0; i < len(listaIndices); i++ {
			if module.final[indicesEmpate[i]].Tempo > maior {
				maior = module.final[indicesEmpate[i]].Tempo
				indiceMaior = i
			}
		}

		novaListaInteiros = append(novaListaInteiros, indicesEmpate[indiceMaior])
		listaIndices = removeIndexInteiros(listaIndices, indiceMaior)

		if len(listaIndices) == 0 {
			break
		}
	}

	return novaListaInteiros

}

// Função para adicionar um TimeStamp a uma mensagem que ja foi adicioanda
func (module *SKEEN_Module) adicionaTimeStamp(message Mensagem) {
	for i := 0; i < len(module.listaDeMensagens); i++ {
		if message.Message == module.listaDeMensagens[i].Message {
			module.listaDeMensagens[i].Tempo = append(module.listaDeMensagens[i].Tempo, message.Tempo)
			module.listaDeMensagens[i].PartitionID = append(module.listaDeMensagens[i].PartitionID, message.PartitionID)
		}
	}
}

func removeIndexInteiros(listaInteiros []int, index int) []int {
	return append(listaInteiros[:index], listaInteiros[index+1])
}

// Função para remover uma mensagem da lista de mensagens local dado um indice
func (module *SKEEN_Module) RemoveIndex(index int) []Local {
	return append(module.listaDeMensagens[:index], module.listaDeMensagens[index+1:]...)
}

// Função para identificar se uma determinada mensagem ja foi recebida antes ou nao
func (module *SKEEN_Module) jaRecebeu(message Mensagem) bool {
	for i := 0; i < len(module.listaDeMensagens); i++ {
		if message.Message == module.listaDeMensagens[i].Message {
			return true
		}
	}
	return false
}

// Função para adicionar a lista de mensagens local uma mensagem
func (module *SKEEN_Module) adicionaMensagemLocal(message Mensagem) {
	var timeStampLocal = []int{module.clock}
	var partitionIDLocal = []int{message.PartitionID}
	var mensagem = Local{
		Message:     message.Message,
		Tempo:       timeStampLocal,
		PartitionID: partitionIDLocal,
	}
	module.listaDeMensagens = append(module.listaDeMensagens, mensagem)
}

// Função para tratar uma mensagem que foi recebida, identificando o que deve ser adicionado, ela toda ou apenas o timestamp
func (module *SKEEN_Module) tratamentoMensagem(mensagemOrignal Mensagem, mensagemASerAdicionada Mensagem) {

	if !module.jaRecebeu(mensagemOrignal) {
		module.adicionaMensagemLocal(mensagemASerAdicionada)
	} else {
		module.adicionaTimeStamp(mensagemASerAdicionada)
	}
}

// Função para identificar se ja foi recebido o timestamp de todos os outros processos para uma determinada mensagem
func (module *SKEEN_Module) checarSeRecebeuDeTodos(message Mensagem) bool {
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
func (module *SKEEN_Module) jaRecebeuDeTodos(message Mensagem) {
	var msgLocal = module.getMensagemDaLista(message)
	var maxTS = getMaxTS(msgLocal)
	module.addFinal(Mensagem{
		Message:     message.Message,
		Tempo:       msgLocal.Tempo[maxTS],
		PartitionID: msgLocal.PartitionID[maxTS],
	})
	var index = module.getIndex(message)

	module.listaDeMensagens = module.RemoveIndex(index)

	module.clock = getMaxClock(module.clock, maxTS)
	module.tryDeliver()
}

// Adiciona uma mensagem a lista Final
func (module *SKEEN_Module) addFinal(message Mensagem) {
	module.final = append(module.final, message)
}

// Função para retornar o indice de uma determinada mensagem na lista de mensagens local
func (module *SKEEN_Module) getIndex(message Mensagem) int {
	for i := 0; i < len(module.listaDeMensagens); i++ {
		if message.Message == module.listaDeMensagens[i].Message {
			return i
		}
	}
	return -1
}

// Função para retornar o indice de uma determinada mensagem na lista de mensagem Final
func (module *SKEEN_Module) getIndexFinal(message Mensagem) int {
	for i := 0; i < len(module.final); i++ {
		if message.Message == module.final[i].Message {
			return i
		}
	}
	return -1
}

// Função para remover uma mensagem da lista Final dado um indice
func (module *SKEEN_Module) RemoveIndexFinal(index int) {
	module.final = append(module.final[:index], module.final[index+1:]...)
}

// Função para retornar o indice da mensagem com menor timestamp ta lista Final
func (module *SKEEN_Module) getMin() int {
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
func (module *SKEEN_Module) temMenorEsperando(tempo int) bool {
	for i := 0; i < len(module.listaDeMensagens); i++ {
		var max = getMaxTS(module.listaDeMensagens[i])
		if module.listaDeMensagens[i].Tempo[max] < tempo {
			return true
		}
	}

	return false
}

// Função para detectar se existe algum empate em relação a decisão do timestamp
func (module *SKEEN_Module) temEmpate(indice int) bool {
	var tempoPesquisado = module.final[indice].Tempo
	for i := 0; i < len(module.final); i++ {
		var tempoPesquisa = module.final[i].Tempo
		if tempoPesquisado == tempoPesquisa && i != indice {
			return true
		}
	}
	return false
}

// Função que dado uma mensagem retorna o que ja está armazenado localmente
func (module *SKEEN_Module) getMensagemDaLista(message Mensagem) Local {
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
			timeStamp = i
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
func (module *SKEEN_Module) receive(message Mensagem) {

	if message.Tempo == CONST_TS_PADRAO {
		module.clock = module.clock + 1
		var msg = Mensagem{
			Message:     message.Message,
			Tempo:       module.clock,
			PartitionID: module.ID,
		}
		module.tratamentoMensagem(message, msg)

		module.Broadcast(SKEEN_Req_Message{
			Addresses: module.addresses,
			Message:   message.Message + "/" + strconv.Itoa(module.clock) + "&" + strconv.Itoa(module.ID),
		})

	} else {

		module.tratamentoMensagem(message, message)
	}

	if module.checarSeRecebeuDeTodos(message) {
		module.jaRecebeuDeTodos(message)
	}
}

// Função que fara as tentativas de entrega das mensagens
func (module *SKEEN_Module) tryDeliver() {
	for {
		if len(module.final) > 0 {
			var indexMin = module.getMin()
			// fmt.Println(module.temEmpate(indexMin))
			if module.temMenorEsperando(indexMin) {
				return
			} else {
				if module.temEmpate(indexMin) {
					var indicesEmpate = module.procuraEmpate(indexMin)
					indicesEmpate = module.decideEmpate(indicesEmpate)
					for i := 0; i < len(indicesEmpate); i++ {
						module.Deliver(SKEEN_Ind_Message{
							Message: module.final[indicesEmpate[i]].Message,
							Tempo:   strconv.Itoa(module.final[indicesEmpate[i]].Tempo),
						})
						module.RemoveIndexFinal(indexMin)
					}
				} else {
					module.Deliver(SKEEN_Ind_Message{
						Message: module.final[indexMin].Message,
						Tempo:   strconv.Itoa(module.final[indexMin].Tempo),
					})
					module.RemoveIndexFinal(indexMin)
				}
			}
		} else {
			return
		}
	}
}

func (module *SKEEN_Module) outDbg(s string) {
	if module.dbg {
		fmt.Println(". . . . . . . . . [ URB msg : " + s + " ]")
	}
}

func (module *SKEEN_Module) Init(address string, addresses []string) {
	module.InitD(address, true, addresses)
}

func (module *SKEEN_Module) InitD(address string, _dbg bool, addresses []string) {

	var partition, err = strconv.Atoi(strings.Split(address, ":")[1])
	_ = err

	module.PartitionID = partition

	module.addresses = addresses
	module.dbg = _dbg
	module.outDbg("Init URB!")
	module.beb = BestEffortBroadcast_Module{
		Req: make(chan BestEffortBroadcast_Req_Message, 10000),
		Ind: make(chan BestEffortBroadcast_Ind_Message, 10000)}
	module.beb.Init(address)

	module.Start()
}

func temPartitionID(message string) bool {

	if len(strings.Split(message, "&")) > 1 {
		return true
	}
	return false
}

func geraMensagem(mensagem mensagemAux) Mensagem {
	if temPartitionID(mensagem.Message) {
		var msg = strings.Split(mensagem.Message, "/")[0]
		var tempoEPartitionID = strings.Split(mensagem.Message, "/")[1]

		var tempo, err = strconv.Atoi(strings.Split(tempoEPartitionID, "&")[0])
		_ = err
		var partitionID, e = strconv.Atoi(strings.Split(tempoEPartitionID, "&")[1])
		_ = e
		return Mensagem{
			Message:     msg,
			Tempo:       tempo,
			PartitionID: partitionID,
		}
	} else {
		var msg = strings.Split(mensagem.Message, "/")[0]
		var tempo, err = strconv.Atoi(strings.Split(mensagem.Message, "/")[1])
		_ = err

		return Mensagem{
			Message: msg,
			Tempo:   tempo,
		}
	}
}

func (module *SKEEN_Module) Start() {
	module.clock = 0
	go func() {
		for {
			select {
			case y := <-module.Req:
				go module.Broadcast(y)
			case y := <-module.beb.Ind:
				var mensagemGerada = geraMensagem(mensagemAux{
					Message: y.Message,
				})
				// fmt.Println(mensagemGerada)
				module.receive(mensagemGerada)
			}

		}
	}()
}

func (module *SKEEN_Module) quit() {
	close(module.Ind)

}

func (module *SKEEN_Module) Broadcast(message SKEEN_Req_Message) {

	req := BestEffortBroadcast_Req_Message{
		Addresses: module.addresses,
		Message:   message.Message}
	module.beb.Req <- req
}

func (module *SKEEN_Module) Deliver(message SKEEN_Ind_Message) {

	// fmt.Println("DELIVERED", message)
	module.Ind <- message
	// fmt.Println("# End BEB Received")
}
