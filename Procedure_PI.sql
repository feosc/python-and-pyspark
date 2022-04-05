CREATE PROCEDURE SP_POPULA_DW_CRYPTOBLUES
AS
BEGIN

	--Limpeza Preventiva

	TRUNCATE TABLE [DW].[DIM_Criptomoeda]
	TRUNCATE TABLE [DW].[DIM_Data]
	TRUNCATE TABLE [DW].[FATO_CotacaoDolar]
	TRUNCATE TABLE [DW].[FATO_Mercado]
	TRUNCATE TABLE [DW].[FATO_BitinfoCharts]
	TRUNCATE TABLE [DW].[FATO_Noticia]

	--Insert na Dimensão Criptomoeda

	INSERT [DW].[DIM_Criptomoeda](NomeCriptomoeda)
	SELECT DISTINCT CRIPTOMOEDA FROM [STG].Mercado

	INSERT [DW].[DIM_Criptomoeda](NomeCriptomoeda)
	SELECT DISTINCT A.CRIPTOMOEDA FROM [STG].BitinfoCharts AS A
	LEFT JOIN [STG].Mercado AS B
	ON A.CRIPTOMOEDA = B.CRIPTOMOEDA 
	WHERE A.CRIPTOMOEDA NOT IN (SELECT DISTINCT NomeCriptomoeda FROM [DW].[DIM_Criptomoeda])

	--Inserção na Dimensão Data

	DECLARE @DATAINICIO DATETIME,
					@DATAFIM    DATETIME,
					@DATA       DATETIME

	PRINT Getdate()
	
	--Selecione o Intervalo de datas que deseja
	SELECT @DATAINICIO = '1/1/2009',
		   @DATAFIM = '1/1/2025'

	SELECT @DATA = @DATAINICIO

	WHILE @DATA < @DATAFIM
	  BEGIN
		  INSERT INTO dw.dim_data
					  (data,
					   dia,
					   diasemana,
					   mes,
					   nomemes,
					   quarto,
					   nomequarto,
					   ano)
		  SELECT @DATA                  AS DATA,
				 Datepart(day, @DATA)   AS DIA,
				 CASE Datepart(dw, @DATA)
				   WHEN 1 THEN 'Domingo'
				   WHEN 2 THEN 'Segunda'
				   WHEN 3 THEN 'Terça'
				   WHEN 4 THEN 'Quarta'
				   WHEN 5 THEN 'Quinta'
				   WHEN 6 THEN 'Sexta'
				   WHEN 7 THEN 'Sábado'
				 END                    AS DIASEMANA,
				 Datepart(month, @DATA) AS MES,
				 CASE Datename(month, @DATA)
				   WHEN 'January' THEN 'Janeiro'
				   WHEN 'February' THEN 'Fevereiro'
				   WHEN 'March' THEN 'Março'
				   WHEN 'April' THEN 'Abril'
				   WHEN 'May' THEN 'Maio'
				   WHEN 'June' THEN 'Junho'
				   WHEN 'July' THEN 'Julho'
				   WHEN 'August' THEN 'Agosto'
				   WHEN 'September' THEN 'Setembro'
				   WHEN 'October' THEN 'Outubro'
				   WHEN 'November' THEN 'Novembro'
				   WHEN 'December' THEN 'Dezembro'
				 END                    AS NOMEMES,
				 Datepart(qq, @DATA)    QUARTO,
				 CASE Datepart(qq, @DATA)
				   WHEN 1 THEN 'Primeiro'
				   WHEN 2 THEN 'Segundo'
				   WHEN 3 THEN 'Terceiro'
				   WHEN 4 THEN 'Quarto'
				 END                    AS NOMEQUARTO,
				 Datepart(year, @DATA)  ANO

		  SELECT @DATA = Dateadd(dd, 1, @DATA)
	  END

	UPDATE dw.dim_data
	SET    dia = '0' + dia
	WHERE  Len(dia) = 1

	UPDATE dw.dim_data
	SET    mes = '0' + mes
	WHERE  Len(mes) = 1

	UPDATE dw.dim_data
	SET    datacompleta = ano + mes + dia

	--Inserção na Tabela Fato Mercado

	INSERT [DW].[FATO_Mercado](dimCriptomoeda_Id, dimData_Id, Abertura, Fechamento, MinDia, MaxDia, Volume, QtdeMoeda, QtdeTransacoes, PrecoMedio)
	SELECT 

		DC.ID,
		DD.ID,
		SM.Abertura,
		SM.Fechamento,
		SM.MinDia,
		SM.MaxDia,
		SM.Volume,
		SM.QtdeMoeda,
		REPLACE(SM.QtdeTransacoes, '.0', '') QtdeTransacoes,
		SM.PrecoMedio

	FROM DW.DIM_Criptomoeda DC
	INNER JOIN STG.Mercado SM
	ON DC.NomeCriptomoeda = SM.Criptomoeda
	INNER JOIN DW.DIM_Data DD
	ON DD.DATA = SM.DATA

	--Inserção na Tabela Fato Cotacao Dolar

	INSERT [DW].[FATO_CotacaoDolar](dimData_Id, CotacaoCompra, CotacaoVenda)
	SELECT

		DD.Id,
		SCD.CotacaoCompra,
		SCD.CotacaoVenda

	FROM STG.CotacaoDolar SCD
	INNER JOIN DW.DIM_Data DD
	ON DD.Data = SCD.Data AND DD.Id NOT IN (SELECT dimData_Id FROM [DW].[FATO_CotacaoDolar])

	--Inserção na Tabela Fato BitinfoCharts

	INSERT [DW].[FATO_BitinfoCharts](dimData_Id, dimCriptomoeda_Id, QtdeTweets,  QtdeTransacoes, QtdeTrends, PrecoMedio)
	SELECT 
	
	    DD.Id,
	    DC.Id,
	    SBC.QtdeTweets,
	    SBC.QtdeTransacoes,
	    SBC.QtdeTrends,
	    SBC.PrecoMedio
	
	FROM DW.DIM_Criptomoeda DC
	INNER JOIN STG.BitinfoCharts SBC
	ON DC.NomeCriptomoeda = SBC.Criptomoeda
	INNER JOIN DW.DIM_Data DD
	ON DD.Data = SBC.Data

--Inserção na Tabela Fato Noticia

	INSERT [DW].[FATO_Noticia] (dimCriptomoeda_Id, dimData_Id, Noticia)
	SELECT
	
			DC.ID,
			DD.ID,
			SN.Noticia
	
	FROM DW.DIM_Criptomoeda DC
	INNER JOIN STG.Noticia SN
	ON DC.NomeCriptomoeda = SN.Criptomoeda
	INNER JOIN DW.DIM_Data DD
	ON DD.Data = SN.Data
	
	--Limpeza Preventiva

	TRUNCATE TABLE [STG].Mercado
	TRUNCATE TABLE [STG].BitinfoCharts
	TRUNCATE TABLE [STG].CotacaoDolar
	TRUNCATE TABLE [STG].Noticia

END
GO