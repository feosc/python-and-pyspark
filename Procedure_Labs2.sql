CREATE PROCEDURE [dolar_felipe_coelho].SP_CONVERT_DOLAR
AS
BEGIN
	
	--Limpeza Preventiva DW
	TRUNCATE TABLE [dolar_felipe_coelho].DOLAR_DW_FELIPE_COELHO

	INSERT [dolar_felipe_coelho].DOLAR_DW_FELIPE_COELHO(cotacaoCompra, cotacaoVenda, dataHoraCotacao)
	SELECT 

		REPLACE(COTACAOCOMPRA, ',', '.') AS CotacaoCompra,
		REPLACE(COTACAOVENDA, ',' ,'.') AS CotacaoVenda,
		CAST(DATAHORACOTACAO AS DATE)

	FROM [dolar_felipe_coelho].[DOLAR_STAGE_FELIPE_COELHO]

	--Limpeza Preventiva Stage
	TRUNCATE TABLE [dolar_felipe_coelho].[DOLAR_STAGE_FELIPE_COELHO]

END
GO