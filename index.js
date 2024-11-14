const ProgressBar = require('progress');
const { Builder, Browser, By, until } = require("selenium-webdriver");
const chrome = require("selenium-webdriver/chrome");
const XLSX = require("xlsx");
const { format, parse } = require("date-fns");
const { Pool } = require("pg");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// config banco de dados
const pool = new Pool({
  user: "postgres",
  host: "192.168.25.83",
  database: "db.pessoaJuridica",
  password: "office",
  port: 5432,
});

// api do e-kontroll com o metodo listar empresas
async function listarEmpresas() {
  try {
    const response = await fetch(
      "https://app.e-kontroll.com.br/api/v1/metodo/listar_empresas",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          api_key: "p2zazIRGQ9mwizXKkmVRBasVVW234DLdKkIpu53Rw8eh6zFpBOLolUWBCZmz",
          api_key_empresa: "yQuZX1A45FYa7gohZvmlHHDsUPvjLnGCTxuXMdae4W8T5x05hgWEvQgtUmxf",
        }),
      }
    );

    if (!response.ok) {
      throw new Error("Network response was not ok " + response.statusText);
    }

    const data = await response.json();
    return data.dados.data.filter(item => item.status_empresa === "A");
  } catch (error) {
    console.error("Erro ao listar empresas:", error.message);
    throw error;
  }
}

async function consultarCNPJ(driver, cnpj, isFirstRun) {
  const data = { CNPJ: cnpj, Socios: [] };

  try {
    await driver.get("https://consultacnpj.com/");
    if (isFirstRun) {
      try {
        const termo = await driver.wait(
          until.elementIsVisible(
            driver.findElement(
              By.xpath('//*[@id="modal"]/div/div[2]/div/div[2]/a[1]')
            )
          ),
          10000
        );
        await termo.click();
      } catch (error) {
        //caso termo for aceito ele nao roda
        console.log("Não foi possível encontrar o termo de aceite ou já foi aceito anteriormente.");
      }
    }

    const inputCnpj = await driver.findElement(
      By.xpath('//*[@id="__layout"]/div/div[2]/div[2]/div[1]/div/div[1]/div[2]/div/div/div/input')
    );
    await inputCnpj.sendKeys(cnpj);

    const buscar = await driver.findElement(
      By.xpath('//*[@id="__layout"]/div/div[2]/div[2]/div[1]/div/div[1]/div[2]/div/div/button')
    );
    await buscar.click();

    async function getTextIfExists(xpath, description) {
      try {
        const element = await driver.wait(
          until.elementLocated(By.xpath(xpath)),
          10000
        );
        const text = await element.getText();
        return text;
      } catch (error) {
        return "Não encontrado";
      }
    }

    const empresa = await getTextIfExists('//*[@id="company-data"]/div[4]/p', "Empresa");
    data.Empresa = empresa;
    console.log(`Empresa: ${empresa}`);

    const cnpjCompleto = await getTextIfExists('//*[@id="company-data"]/div[3]/div[1]/p', "CNPJ");
    data.CNPJ = cnpjCompleto;
    console.log(`CNPJ: ${cnpjCompleto}`);

    const fundacao = await getTextIfExists('//*[@id="company-data"]/div[3]/div[2]/p', "Data de Abertura");
    
    if (fundacao !== "Não encontrado") {
      const fundacaoFormat = parse(fundacao, "dd/MM/yyyy", new Date());
      data.DataDeAbertura = format(fundacaoFormat, "yyyy-MM-dd");
      console.log(`Data de Abertura: ${fundacao}`);
    } else {
      console.log(`Data de Abertura: Não encontrado`);
    }
    
    // for q pega quantos socios tiver 
    for (let i = 1; i <= 20; i++) {
      const socio = await getTextIfExists(`//*[@id="company-data"]/div[13]/div/div[${i}]/div[1]/div[1]/p`, `Sócio ${i}`);
      
      if (socio === "Não encontrado") {
        console.log(`Sócio ${i} não encontrado.`);
        break;
      }
      // qualificaçao do socio se ele é adm ou socio etc
      const qualificacao = await getTextIfExists(`//*[@id="company-data"]/div[13]/div/div[${i}]/div[1]/div[2]/div/p`, `Qualificação ${i}`);
      
      console.log(`Sócio ${i}: ${socio}`);
      console.log(`Qualificação ${i}: ${qualificacao}`);
      
      data.Socios.push(`${socio}$${qualificacao}`);
    }

    return data;

  } catch (error) {
    console.error("Ocorreu um erro ao consultar o CNPJ: ", error);
    return data;
  }
}

async function bot() {  
  const options = new chrome.Options();
  options.addArguments("--ignore-certificate-errors");
  options.addArguments("--ignore-ssl-errors");

  let driver = await new Builder()
    .forBrowser(Browser.CHROME)
    .setChromeOptions(options)
    .build();

  const results = [];

  try {
    await driver.manage().window().maximize();

    const empresas = await listarEmpresas();
    const cnpjs = empresas.map(emp => emp.inscricao_federal);
    
    // barra de progresso, q aparece no console para saber quantas empresas entraram no banco
    const bar = new ProgressBar('Processando CNPJs [:bar] :current/:total (:percent) - Tempo restante: :etas', {
      complete: '█',
      incomplete: '░',
      width: 40,
      total: cnpjs.length
    });

    let isFirstAccess = true;

    for (const cnpj of cnpjs) {
      console.log(`\nConsultando CNPJ: ${cnpj}`);

      try {
        const data = await consultarCNPJ(driver, cnpj, isFirstAccess);
        
        if (data.Empresa !== "Não encontrado" && data.CNPJ !== "Não encontrado") {
          results.push(data);
        }

        isFirstAccess = false;

        bar.tick();

        await sleep(60000);
      } catch (cnpjError) {
        console.error(`Erro no processamento do CNPJ ${cnpj}:`, cnpjError);
        bar.tick(); 
      }
    }
  } catch (error) {
    console.error("Ocorreu um erro no bot: ", error);
  } finally {
    console.log("\nResultados encontrados:", results.length);
    await driver.quit();
    console.log("Chrome fechado.");

    const maxSocios = Math.max(...results.map(r => r.Socios.length));

  try {
    const client = await pool.connect();

    //criando a tabela no banco 
    let createTableQuery = `
      CREATE TABLE IF NOT EXISTS Validar_socios (
        id SERIAL PRIMARY KEY,
        nome VARCHAR(255),
        cnpj VARCHAR(14),
        data_abertura DATE
    `;
    for (let i = 1; i <= maxSocios; i++) {
      createTableQuery += `, socio_${i} VARCHAR(255)`;
    }
    createTableQuery += `)`;

    await client.query(createTableQuery);

    // insertando no banco 
    const insertQuery = `
      INSERT INTO Validar_socios (nome, cnpj, data_abertura${Array.from({ length: maxSocios }, (_, i) => `, socio_${i + 1}`).join('')})
      VALUES (${Array.from({ length: maxSocios + 3 }, (_, i) => `$${i + 1}`).join(', ')})
    `;

    const insertValues = results.map(obj => {
      const { Empresa, CNPJ, DataDeAbertura, Socios } = obj;

      if (!CNPJ || CNPJ === "Não encontrado" || !Empresa || Empresa === "Não encontrado") {
        console.log(`Dados insuficientes para o CNPJ: ${CNPJ}. Ignorando inserção no banco.`);
        return null; 
      }

      const sociosArray = Socios.map(socio => {
        const [nome, qualificacao] = socio.split('$');
        return `${nome}$${qualificacao}`;
      }).concat(Array(maxSocios - Socios.length).fill(""));
      const cnpjLimpo = CNPJ.replace(/\D/g, '').padStart(14, '0').slice(0, 14);

      return [Empresa, cnpjLimpo, DataDeAbertura, ...sociosArray];
    });

    for (const values of insertValues.filter(values => values !== null)) {
      await client.query(insertQuery, values);
    }

    console.log("Dados inseridos na tabela validar_socios com sucesso");
    client.release();
  } catch (error) {
    console.error("Erro ao criar tabela ou inserir dados no banco de dados:", error);
  } finally {
    await pool.end();
  }
}}


// Executa o bot
bot().catch(console.error);