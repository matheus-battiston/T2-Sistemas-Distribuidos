import sys

def getResultados(listaRespostas: list[str]) -> list[list[int]]:
    listasDeResultados = []
    for nomeArquivo in listaRespostas:
        retirarLinha = []
        with open(nomeArquivo) as f:
            lines = f.readlines()
            for linhas in lines:
                retirarLinha.append(int(linhas.strip("\n")))
                retirarLinha.sort()
            listasDeResultados.append(retirarLinha)

    return listasDeResultados


def compararRespostas(listaRespostas: list[list[int]]) -> bool:
    return all(element == listaRespostas[0] for element in listaRespostas)


if __name__ == '__main__':
    entradas = sys.argv[1:]
    res = getResultados(entradas)

    print(compararRespostas(res))
