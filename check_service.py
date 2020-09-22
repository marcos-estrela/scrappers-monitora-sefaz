# TODO: Validador de estrutura do site (Enviar alguma mensagem caso tenha problema)
# TODO: Separação de estrutura entre os estados
# TODO: Salvar estrutura com horário

import scrapy
import pymongo
from decouple import config

statusAtual = {}

class ScraperSefaz(scrapy.Spider):
    name = 'scrape-table'
    allowed_domains = [
        'https://www.nfe.fazenda.gov.br/portal/disponibilidade.aspx']
    start_urls = ['https://www.nfe.fazenda.gov.br/portal/disponibilidade.aspx']

    def start_requests(self):
        urls = [
            'https://www.nfe.fazenda.gov.br/portal/disponibilidade.aspx',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        api = config('MONGODB_URI')
        client = pymongo.MongoClient(api)
        db = client['monitora-sefaz']
        collection = db['current_status']

        for linha in response.xpath('//*[@class="linhaImparCentralizada" or @class="linhaParCentralizada"]'):
            estado = linha.xpath('td//text()')[0].extract()

            autorizacao = self.verificaStatus(linha.xpath('td')[1].extract())
            retorno_autorizacao = self.verificaStatus(
                linha.xpath('td')[2].extract())
            inutilizacao = self.verificaStatus(linha.xpath('td')[3].extract())
            consulta_protocolo = self.verificaStatus(
                linha.xpath('td')[4].extract())
            status_servico = self.verificaStatus(
                linha.xpath('td')[5].extract())
            consulta_cadastro = self.verificaStatus(
                linha.xpath('td')[7].extract())
            recepcao_evento = self.verificaStatus(
                linha.xpath('td')[8].extract())

            statusAtual[estado] = {
                'autorizacao': autorizacao,
                'retorno_autorizacao': retorno_autorizacao,
                'inutilizacao': inutilizacao,
                'consulta_protocolo': consulta_protocolo,
                'status_servico': status_servico,
                'consulta_cadastro': consulta_cadastro,
                'recepcao_evento': recepcao_evento
            }
        collection.insert_one(statusAtual)

    def verificaStatus(self, coluna):
        status = 'Indefinido'
        if "verde" in coluna:
            status = 'Verde'
        elif "amarelo" in coluna:
            status = 'Amarelo'
        elif "vermelho" in coluna:
            status = 'Vermelho'
        return status
