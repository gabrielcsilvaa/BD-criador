const { Builder, Browser, By, until } = require("selenium-webdriver");
const chrome = require("selenium-webdriver/chrome");
const XLSX = require("xlsx");
const { format, parse } = require("date-fns");
const { Pool } = require("pg");


function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}


const pool = new Pool({
  user: "postgres",
  host: "192.168.25.83", 
  database: "db.pessoaJuridica",
  password: "office",
  port: 5432,
});

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
      const termo = await driver.wait(
        until.elementIsVisible(
          driver.findElement(
            By.xpath('//*[@id="modal"]/div/div[2]/div/div[2]/a[1]')
          )
        ),
        10000
      );
      await termo.click();
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
        console.log(`${description}: ${text}`);
        return text;
      } catch (error) {
        console.log(`${description} não encontrado.`);
        return "Não encontrado";
      }
    }

    data.Empresa = await getTextIfExists('//*[@id="company-data"]/div[4]/p', "Empresa");
    data.CNPJ = await getTextIfExists('//*[@id="company-data"]/div[3]/div[1]/p', "CNPJ");
    const fundacao = await getTextIfExists('//*[@id="company-data"]/div[3]/div[2]/p', "Data de Abertura");
    const fundacaoFormat = parse(fundacao, "dd/MM/yyyy", new Date());
    data.DataDeAbertura = format(fundacaoFormat, "yyyy-MM-dd");

    
    for (let i = 1; i <= 20; i++) {
      const socio = await getTextIfExists(`//*[@id="company-data"]/div[13]/div/div[${i}]/div[1]/div[1]/p`, `Sócio ${i}`);
      if (socio === "Não encontrado") break;
      data.Socios.push(socio);
    }
  } catch (error) {
    console.error("Ocorreu um erro ao consultar o CNPJ: ", error);
  }

  return data;
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
    let isFirstRun = true;

    for (const cnpj of cnpjs) {
      console.log(`Consultando CNPJ: ${cnpj}`);

      const data = await consultarCNPJ(driver, cnpj, isFirstRun);
      if (data.Empresa !== "Não encontrado" && data.CNPJ !== "Não encontrado") {
        results.push(data);
      }
      isFirstRun = false;

      await sleep(60000); 
    }
  } catch (error) {
    console.error("Ocorreu um erro no bot: ", error);
  } finally {
    console.log(results);
    await driver.quit();
    console.log("Chrome fechado.");
  }


  const maxSocios = Math.max(...results.map(r => r.Socios.length));

  try {
    const client = await pool.connect();

    
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

      const sociosArray = Socios.concat(Array(maxSocios - Socios.length).fill("")); //erro
      const cnpjLimpo = CNPJ.replace(/\D/g, '').padStart(14, '0').slice(0, 14);

      return [Empresa, cnpjLimpo, DataDeAbertura, ...sociosArray];
    });

    for (const values of insertValues.filter(values => values !== null)) {
      await client.query(insertQuery, values);
    }

    console.log("Dados inseridos na tabela Validar_socios com sucesso");
    client.release();
  } catch (error) {
    console.error("Erro ao criar tabela ou inserir dados no banco de dados:", error);
  } finally {
    await pool.end();
  }
}

bot();