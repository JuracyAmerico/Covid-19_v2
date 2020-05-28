/*
 * Coleta dados da Secretarias de Saúde das Unidades Federativas, dados tratados por Álvaro Justen e colaboradores/Brasil.IO
 * Para mais detalhes acesse aqui https://brasil.io/api/dataset/covid19 para mais detalhes da documentação da API
 * Mais detalhes sobre o Tableau Web Data Connector acesse aqui https://tableau.github.io/webdataconnector/docs/
 * 
 * @Autor Juracy Americo <jamerico@tableau.com>
 */

(function() {
    //1: *************************
    // Criando o objeto da conexão
    var myConnector = tableau.makeConnector();
	var myProxy = 'http://files.tableaujunkie.com/proxy/proxy.php?u='

    //2: *************************
    // Definição do esquema
    myConnector.getSchema = function(schemaCallback) {
        var casos_colunas = [            
        {id:"city", alias: "Cidade", dataType: tableau.dataTypeEnum.string, geoRole: tableau.geographicRoleEnum.county},
        {id:"city_ibge_code", alias: "Cd Mun", dataType: tableau.dataTypeEnum.string, columnRole: tableau.columnRoleEnum.dimension},
        {id:"confirmed", alias: "Confirmados", dataType: tableau.dataTypeEnum.float},
        {id:"confirmed_per_100k_inhabitants", alias: "Confirmados por 100k hab.", dataType: tableau.dataTypeEnum.int},
        {id:"date", alias: "Data", dataType: tableau.dataTypeEnum.date},
        {id:"death_rate", alias: "Letalidade", dataType: tableau.dataTypeEnum.float, aggType: tableau.aggTypeEnum.avg, numberFormat: tableau.numberFormatEnum.percentage},
        {id:"deaths", alias: "Óbitos", dataType: tableau.dataTypeEnum.float},
        {id:"estimated_population_2019", alias: "População estimada 2019", dataType: tableau.dataTypeEnum.int},
        {id:"is_last", alias: "É a última atualização?", dataType: tableau.dataTypeEnum.bool},
        {id:"place_type", alias: "Tipo de local", dataType: tableau.dataTypeEnum.string},
        {id:"state", alias: "Estado", dataType: tableau.dataTypeEnum.string, geoRole: tableau.geographicRoleEnum.state_province}
        ];

        var boletim_colunas = [
        {id:"date", alias: "Data", dataType: tableau.dataTypeEnum.date},
        {id:"notes", alias: "Notes", dataType: tableau.dataTypeEnum.string},
        {id:"state", alias: "Estado", dataType: tableau.dataTypeEnum.string, geoRole: tableau.geographicRoleEnum.state_province},
        {id:"url", alias: "Url", dataType: tableau.dataTypeEnum.string}
        ];

        var caso_full_colunas = [
        {id:"city", alias:"Cidade", dataType: tableau.dataTypeEnum.string, geoRole: tableau.geographicRoleEnum.county},
        {id:"city_ibge_code", alias:"Cd Mun", dataType: tableau.dataTypeEnum.string, columnRole: tableau.columnRoleEnum.dimension},
        {id:"date", alias:"Data", dataType: tableau.dataTypeEnum.date},
        {id:"epidemiological_week", alias:"Semana Epidemiológica", dataType: tableau.dataTypeEnum.int},
        {id:"estimated_population_2019", alias:"População Estimada", dataType: tableau.dataTypeEnum.int},
        {id:"is_last", alias:"É a última atualização?", dataType: tableau.dataTypeEnum.bool},
        {id:"is_repeated", alias:"É repetido", dataType: tableau.dataTypeEnum.bool},
        {id:"last_available_confirmed", alias:"Confirmados do Último dia disponível", dataType: tableau.dataTypeEnum.int},
        {id:"last_available_confirmed_per_100k_inhabitants", alias:"Confirmados do Último dia disponível por 100.000 habitantes", dataType: tableau.dataTypeEnum.float},
        {id:"last_available_date", alias:"Data da qual o dado se refere", dataType: tableau.dataTypeEnum.date},
        {id:"last_available_death_rate", alias:"Taxa de Fatalidade do Último Dia Disponível", dataType: tableau.dataTypeEnum.float, aggType: tableau.aggTypeEnum.avg, numberFormat: tableau.numberFormatEnum.percentage},
        {id:"last_available_deaths", alias:"Fatalidade do Último Dia Disponível", dataType: tableau.dataTypeEnum.int},
        {id:"new_confirmed", alias:"Novos Casos", dataType: tableau.dataTypeEnum.int},
        {id:"new_deaths", alias:"Novos Óbitos", dataType: tableau.dataTypeEnum.int},
        {id:"order_for_place", alias:"Ordem do Registro", dataType: tableau.dataTypeEnum.int},
        {id:"place_type", alias:"Tipo de local", dataType: tableau.dataTypeEnum.string},
        {id:"state", alias:"Estado", dataType: tableau.dataTypeEnum.string, geoRole: tableau.geographicRoleEnum.state_province}                      
        ]

        var obito_cartorio_colunas = [
        {id:"date", alias:"Data ", dataType: tableau.dataTypeEnum.date},
        {id:"deaths_covid19", alias:"Óbitos Covid19", dataType: tableau.dataTypeEnum.int},
        {id:"deaths_pneumonia_2019", alias:"Óbitos Pneumonia 2019", dataType: tableau.dataTypeEnum.int},
        {id:"deaths_pneumonia_2020", alias:"Óbitos Pneumonia 2020", dataType: tableau.dataTypeEnum.int},
        {id:"deaths_respiratory_failure_2019", alias:"Óbitos Insuficiência Respiratória 2019", dataType: tableau.dataTypeEnum.int},
        {id:"deaths_respiratory_failure_2020", alias:"Óbitos Insuficiência Respiratória 2020", dataType: tableau.dataTypeEnum.int},
        {id:"epidemiological_week_2019", alias:"Semana Epidemiológica 2019", dataType: tableau.dataTypeEnum.int},
        {id:"epidemiological_week_2020", alias:"Semana Epidemiológica 2020", dataType: tableau.dataTypeEnum.int},
        {id:"new_deaths_covid19", alias:"Novos Óbitos Covid19", dataType: tableau.dataTypeEnum.int},
        {id:"new_deaths_pneumonia_2019", alias:"Novos Óbitos Pneumonia 2019", dataType: tableau.dataTypeEnum.int},
        {id:"new_deaths_pneumonia_2020", alias:"Novos Óbitos Pneumonia 2020", dataType: tableau.dataTypeEnum.int},
        {id:"new_deaths_respiratory_failure_2019", alias:"Novos Óbitos Insuficiência Respiratória 2019", dataType: tableau.dataTypeEnum.int},
        {id:"new_deaths_respiratory_failure_2020", alias:"Novos Óbitos Insuficiência Respiratória 2020", dataType: tableau.dataTypeEnum.int},
        {id:"state", alias:"Estado", dataType: tableau.dataTypeEnum.string, geoRole: tableau.geographicRoleEnum.state_province}
        ]

        //Definicao da Tabela.
        var casos_tabela = {
            id: 'casos',
            alias: 'casos',
            description: 'Essa tabela tem apenas os casos relatados pelos boletins das Secretarias Estaduais de Saúde e, por isso, não possui valores para todos os municípios e todas as datas - é nossa "tabela canônica", que reflete o que foi publicado.',
            columns: casos_colunas
        };

        var boletim_tabela = {
            id: 'boletim',
            alias: 'boletim',
            description: 'Tabela que lista os boletins publicados pelas Secretarias Estaduais de Saúde. Pode aparecer mais de um para a mesma data e podem existir dias em que as SES não publicam boletins.',
            columns: boletim_colunas
        };

        var caso_full_tabela = {
            id: 'caso_full',
            alias: 'caso_full',
            description: 'Tabela gerada a partir da tabela caso, que possui um registro por município (+ Importados/Indefinidos) e estado para cada data disponível; nos casos em que um boletim não foi divulgado naquele dia, é copiado o dado do último dia disponível e a coluna is_repeated fica com o valor True.',
            columns: caso_full_colunas
        };
        
        var obito_cartorio_tabela = {
            id: 'obito_cartorio',
            alias: 'obito_cartorio',
            description: 'Essa tabela contém dados de óbitos registrados nos cartórios e disponíveis no Portal da Transparência do Registro Civil.',
            columns: obito_cartorio_colunas
        };        


        schemaCallback([casos_tabela, boletim_tabela, caso_full_tabela, obito_cartorio_tabela]);
    };

    //3: *************************
        // Busca e Download dos dados
    myConnector.getData = function(table, doneCallback) {  
        //Acessando dados de casos
        if (table.tableInfo.id == 'casos') {        
            function getAllData(url) {  
            $.getJSON(url, function(resp) {  
                var feat = resp.results,  
                next = resp.next;  
        
                // Interagindo no objeto JSON  
        
                for (var i = 1, len = feat.length; i < len; i++) {  
                tableData.push({  
                    "city": feat[i].city,
                    "city_ibge_code": feat[i].city_ibge_code,
                    "confirmed": feat[i].confirmed,
                    "confirmed_per_100k_inhabitants": feat[i].confirmed_per_100k_inhabitants,
                    "date": feat[i].date,
                    "death_rate": feat[i].death_rate,
                    "deaths": feat[i].deaths,
                    "estimated_population_2019": feat[i].estimated_population_2019,
                    "is_last": feat[i].is_last,
                    "place_type": feat[i].place_type,
                    "state": feat[i].state 
                });  
                }  
                // Interagindo entre todas as paginas, para isso fazemos a variavel next = resp.next; para pegar proxima pagina com dados , se nao encontrar mais dados vai ser null
                // https://community.tableau.com/thread/335501 contribuição da Keshia Rose
                if (next !== null) {  
                getAllData(next);  
                } else {  
                table.appendRows(tableData);  
                doneCallback();  
                }  
            });  
            }
        
      
            var tableData = [];  
            var url = "https://brasil.io/api/dataset/covid19/caso/data?page=1";  
            getAllData(url);  
        } 

        //Acessando dados dos boletim
        if (table.tableInfo.id == 'boletim') {        
            function getAllData(url) {  
            $.getJSON(url, function(resp) {  
                var feat = resp.results,  
                next = resp.next;  
        
                // Interagindo no objeto JSON  
        
                for (var i = 1, len = feat.length; i < len; i++) {  
                tableData.push({  
                    "date": feat[i].date,
                    "notes": feat[i].notes,
                    "state": feat[i].state,
                    "url": feat[i].url
                     
                });  
                }  
                // Interagindo entre todas as paginas, para isso fazemos a variavel next = resp.next; para pegar proxima pagina com dados , se nao encontrar mais dados vai ser null
                // https://community.tableau.com/thread/335501 contribuição da Keshia Rose
                if (next !== null) {  
                getAllData(next);  
                } else {  
                table.appendRows(tableData);  
                doneCallback();  
                }  
            });  
            }
        
      
            var tableData = [];  
            var url = "https://brasil.io/api/dataset/covid19/boletim/data?page=1";  
            getAllData(url);  
        }
        
        //Acessando dados dos caso_full
        if (table.tableInfo.id == 'caso_full') {        
            function getAllData(url) {  
            $.getJSON(url, function(resp) {  
                var feat = resp.results,  
                next = resp.next;  
        
                // Interagindo no objeto JSON  
        
                for (var i = 1, len = feat.length; i < len; i++) {  
                tableData.push({  
                    "city": feat[i].city,
                    "city_ibge_code": feat[i].city_ibge_code,
                    "date": feat[i].date,
                    "epidemiological_week": feat[i].epidemiological_week,
                    "estimated_population_2019": feat[i].estimated_population_2019,
                    "is_last": feat[i].is_last,
                    "is_repeated": feat[i].is_repeated,
                    "last_available_confirmed": feat[i].last_available_confirmed,
                    "last_available_confirmed_per_100k_inhabitants": feat[i].last_available_confirmed_per_100k_inhabitants,
                    "last_available_date": feat[i].last_available_date,
                    "last_available_death_rate": feat[i].last_available_death_rate,
                    "last_available_deaths": feat[i].last_available_deaths,
                    "new_confirmed": feat[i].new_confirmed,
                    "new_deaths": feat[i].new_deaths,
                    "order_for_place": feat[i].order_for_place,
                    "place_type": feat[i].place_type,
                    "state": feat[i].state
                                                             
                });  
                }  
                // Interagindo entre todas as paginas, para isso fazemos a variavel next = resp.next; para pegar proxima pagina com dados , se nao encontrar mais dados vai ser null
                // https://community.tableau.com/thread/335501 contribuição da Keshia Rose
                if (next !== null) {  
                getAllData(next);  
                } else {  
                table.appendRows(tableData);  
                doneCallback();  
                }  
            });  
            }
        
      
            var tableData = [];  
            var url = "https://brasil.io/api/dataset/covid19/caso_full/data?page=1";  
            getAllData(url);  
        }          

        //Acessando dados dos obito_cartorio
        if (table.tableInfo.id == 'obito_cartorio') {        
            function getAllData(url) {  
            $.getJSON(url, function(resp) {  
                var feat = resp.results,  
                next = resp.next;  
        
                // Interagindo no objeto JSON  
        
                for (var i = 1, len = feat.length; i < len; i++) {  
                tableData.push({  
                    "date": feat[i].date,
                    "deaths_covid19": feat[i].deaths_covid19,
                    "deaths_pneumonia_2019": feat[i].deaths_pneumonia_2019,
                    "deaths_pneumonia_2020": feat[i].deaths_pneumonia_2020,
                    "deaths_respiratory_failure_2019": feat[i].deaths_respiratory_failure_2019,
                    "deaths_respiratory_failure_2020": feat[i].deaths_respiratory_failure_2020,
                    "epidemiological_week_2019": feat[i].epidemiological_week_2019,
                    "epidemiological_week_2020": feat[i].epidemiological_week_2020,
                    "new_deaths_covid19": feat[i].new_deaths_covid19,
                    "new_deaths_pneumonia_2019": feat[i].new_deaths_pneumonia_2019,
                    "new_deaths_pneumonia_2020": feat[i].new_deaths_pneumonia_2020,
                    "new_deaths_respiratory_failure_2019": feat[i].new_deaths_respiratory_failure_2019,
                    "new_deaths_respiratory_failure_2020": feat[i].new_deaths_respiratory_failure_2020,
                    "state": feat[i].state
                                         
                });  
                }  
                // Interagindo entre todas as paginas, para isso fazemos a variavel next = resp.next; para pegar proxima pagina com dados , se nao encontrar mais dados vai ser null
                // https://community.tableau.com/thread/335501 contribuição da Keshia Rose
                if (next !== null) {  
                getAllData(next);  
                } else {  
                table.appendRows(tableData);  
                doneCallback();  
                }  
            });  
            }
        
      
            var tableData = [];  
            var url = "https://brasil.io/api/dataset/covid19/obito_cartorio/data?page=1";  
            getAllData(url);  
        }        
    }; 

    //4: *************************
    tableau.registerConnector(myConnector);

    //5: *************************
    // Criação do evento que fica escutando quando o usuário submete o formulário
    $(document).ready(function() {
        $("#submitButton").click(function() {
            tableau.connectionName = "Covid-19"; // Este texto vai ser o nome na fonte de dados no Tableau
            tableau.submit(); // Este comando envia o objeto conexão criado no inicio para o Tableau
        });
    });
})();