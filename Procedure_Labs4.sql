CREATE PROCEDURE SP_POPULADW_FELIPE_MUSEU
AS
BEGIN
	--Limpeza Preventiva Stages

	TRUNCATE TABLE DW_FEC.DIM_TipoMuseu
	TRUNCATE TABLE DW_FEC.DIM_Regiao
	TRUNCATE TABLE DW_FEC.DIM_Museu
	TRUNCATE TABLE DW_FEC.FATO_Eventos
	
	-- Update table stage

	UPDATE STAGES_FEC.STG_Space
	SET UF = 'AL'
	WHERE UF = 'Alagoas'

	-- Insert Dimens�o Tipo do Museu

	INSERT DW_FEC.DIM_TipoMuseu(TIPO)
	SELECT DISTINCT TIPO1 FROM STAGES_FEC.STG_Space

	INSERT DW_FEC.DIM_TipoMuseu(TIPO)
	SELECT DISTINCT TIPO2 FROM STAGES_FEC.STG_Space
	WHERE TIPO2 NOT IN (SELECT TIPO FROM DW_FEC.DIM_TipoMuseu)

	INSERT DW_FEC.DIM_TipoMuseu(TIPO)
	SELECT DISTINCT TIPO3 FROM STAGES_FEC.STG_Space
	WHERE TIPO3 NOT IN (SELECT TIPO FROM DW_FEC.DIM_TipoMuseu)

	-- Insert Dimensao Regiao

	INSERT DW_FEC.DIM_Regiao(ESTADO)
	SELECT DISTINCT UF FROM STAGES_FEC.STG_Space

	UPDATE DW_FEC.DIM_Regiao
	SET REGIAO = 'NORTE'
	WHERE ESTADO = 'AC' OR ESTADO ='AM' OR ESTADO = 'AP' OR ESTADO= 'PA' 
					OR ESTADO= 'RO' OR ESTADO= 'RR' OR ESTADO= 'TO'

	UPDATE DW_FEC.DIM_Regiao
	SET REGIAO = 'NORDESTE'
	WHERE ESTADO = 'AL' OR ESTADO ='BA' OR ESTADO = 'CE' OR ESTADO= 'MA' OR ESTADO= 'SE'
					OR ESTADO= 'PB' OR ESTADO= 'PE' OR ESTADO= 'PI' OR ESTADO= 'RN'

	UPDATE DW_FEC.DIM_Regiao
	SET REGIAO = 'CENTRO-OESTE'
	WHERE ESTADO = 'GO' OR ESTADO ='MS' OR ESTADO = 'MT' OR ESTADO= 'DF'

	UPDATE DW_FEC.DIM_Regiao
	SET REGIAO = 'SUDESTE'
	WHERE ESTADO = 'SP' OR ESTADO ='RJ' OR ESTADO = 'ES' OR ESTADO= 'MG'

	UPDATE DW_FEC.DIM_Regiao
	SET REGIAO = 'SUL'
	WHERE ESTADO = 'PR' OR ESTADO ='RS' OR ESTADO = 'SC'

	--Insert Dim Museu

	INSERT DW_FEC.DIM_Museu(ID, ID_REGIAO, ID_TIPOM, NOME, ENDERECO, DESCRICAO, LATITUDE, LONGITUDE)
	SELECT 
	
		STS.ID_MUSEU,
		DR.ID,
		DT.ID,
		STS.NOME,
		STS.ENDERECO,
		STS.DESCRICAO,
		STS.LATITUDE,
		STS.LONGITUDE

	FROM DW_FEC.DIM_Regiao DR
	INNER JOIN STAGES_FEC.STG_Space STS
	ON DR.ESTADO = STS.UF
	INNER JOIN DW_FEC.DIM_TipoMuseu DT
	ON DT.TIPO = STS.TIPO1

	--Insert Fato Evento

	INSERT DW_FEC.FATO_Eventos(ID_MUSEU, DESCRICAO, DATAINICIO, DESCRICAO_EVENT)
	SELECT 

		ID_MUSEU,
		DESDRICAO,
		DATA_INICIO,
		DESCRICAO_EVENT

	FROM STAGES_FEC.STG_Event

	--Limpeza Preventiva Stages

	TRUNCATE TABLE STAGES_FEC.STG_Event
	TRUNCATE TABLE STAGES_FEC.STG_Space


END