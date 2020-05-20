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
        {id:"city", alias: "Cidade", dataType: tableau.dataTypeEnum.string, geoRole: tableau.geographicRoleEnum.county , description: "nome do município (pode estar em branco quando o registro é referente ao estado, pode ser preenchido com Importados/Indefinidos também"},
        {id:"city_ibge_code", alias: "Codigo IBGE", dataType: tableau.dataTypeEnum.int, columnRole: tableau.columnRoleEnum.dimension},
        {id:"confirmed", alias: "Confirmados", dataType: tableau.dataTypeEnum.float},
        {id:"confirmed_per_100k_inhabitants", alias: "Confirmados por 100k hab.", dataType: tableau.dataTypeEnum.int},
        {id:"date", alias: "Data", dataType: tableau.dataTypeEnum.date},
        {id:"death_rate", alias: "Mortes/confirmados", dataType: tableau.dataTypeEnum.float},
        {id:"deaths", alias: "Fatalidades", dataType: tableau.dataTypeEnum.float},
        {id:"estimated_population_2019", alias: "População estimada 2019", dataType: tableau.dataTypeEnum.int},
        {id:"is_last", alias: "É a última atualização?", dataType: tableau.dataTypeEnum.string},
        {id:"place_type", alias: "Tipo de local", dataType: tableau.dataTypeEnum.string},
        {id:"state", alias: "Estado", dataType: tableau.dataTypeEnum.string, geoRole: tableau.geographicRoleEnum.state_province}
        ];

        var boletim_colunas = [
        {id:"date", alias: "Data", dataType: tableau.dataTypeEnum.date},
        {id:"notes", alias: "Notes", dataType: tableau.dataTypeEnum.string},
        {id:"state", alias: "Estado", dataType: tableau.dataTypeEnum.string},
        {id:"url", alias: "Url", dataType: tableau.dataTypeEnum.string}
        ];

        var obito_cartorio_colunas = [
        {id:"date", alias:"date", dataType: tableau.dataTypeEnum.date},
        {id:"deaths_covid19", alias:"deaths_covid19", dataType: tableau.dataTypeEnum.int},
        {id:"deaths_pneumonia_2019", alias:"deaths_pneumonia_2019", dataType: tableau.dataTypeEnum.int},
        {id:"deaths_pneumonia_2020", alias:"deaths_pneumonia_2020", dataType: tableau.dataTypeEnum.int},
        {id:"deaths_respiratory_failure_2019", alias:"deaths_respiratory_failure_2019", dataType: tableau.dataTypeEnum.int},
        {id:"deaths_respiratory_failure_2020", alias:"deaths_respiratory_failure_2020", dataType: tableau.dataTypeEnum.int},
        {id:"epidemiological_week_2019", alias:"epidemiological_week_2019", dataType: tableau.dataTypeEnum.int},
        {id:"epidemiological_week_2020", alias:"epidemiological_week_2020", dataType: tableau.dataTypeEnum.int},
        {id:"new_deaths_covid19", alias:"new_deaths_covid19", dataType: tableau.dataTypeEnum.int},
        {id:"new_deaths_pneumonia_2019", alias:"new_deaths_pneumonia_2019", dataType: tableau.dataTypeEnum.int},
        {id:"new_deaths_pneumonia_2020", alias:"new_deaths_pneumonia_2020", dataType: tableau.dataTypeEnum.int},
        {id:"new_deaths_respiratory_failure_2019", alias:"new_deaths_respiratory_failure_2019", dataType: tableau.dataTypeEnum.int},
        {id:"new_deaths_respiratory_failure_2020", alias:"new_deaths_respiratory_failure_2020", dataType: tableau.dataTypeEnum.int},
        {id:"state", alias:"state", dataType: tableau.dataTypeEnum.string}
        ]

        //Definicao da Tabela
        var casosTabela = {
            id: 'casos',
            alias: 'casos',
            columns: casos_colunas
        };

        var boletim_colunasTabela = {
            id: 'boletim',
            alias: 'boletim',
            columns: boletim_colunas
        };

        var obito_cartorio_colunasTabela = {
            id: 'obito_cartorio',
            alias: 'obito_cartorio',
            columns: obito_cartorio_colunas
        };        


        schemaCallback([casosTabela, boletim_colunasTabela, obito_cartorio_colunasTabela]);
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