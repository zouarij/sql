INSERT INTO #TRANSACTION_TMP (
    [TRA_CodEtat], [TRA_TypSrc], [TRA_IdSrc], TRA_Workflow, [TRA_CodSociete], 
    [TRA_CodIdentifiantClient], [TRA_IdTiersDecisionnaire], [TRA_CanalOrdreClient], 
    [TRA_CodPrestataireOrigine], [TRA_CodPrestataire], [TRA_MarcheOTC], 
    [TRA_TypTiersContrepartie], [TRA_IdTiersContrepartie], [TRA_CodTypDistribution], 
    [TRA_IdOrdreMarche], [TRA_IdExecution], [TRA_IdExterneTransactionMarche], 
    [TRA_IdCollaborateurExec], [TRA_IdAlgorithme], [TRA_TradingDateTime], 
    [TRA_IdInstrument], [TRA_CodSens], [TRA_IsVenteADecouvert], [TRA_Qte], 
    [TRA_DeviseQte], [TRA_Prix], [TRA_DevisePrix], [TRA_MontantNet], 
    TRA_TransmitterCodLEI, TRA_Counterparty_Id, TRA_ExecId, TRA_TradeId, 
    TRA_OrderId, TRA_BrokerId, TRA_lastMarket
)
SELECT distinct
    'CONSO',
    'ULL',
    table_id,
    'b_i/market/' + isnull(ownerdesk_op,'') + '/' + isnull(ordercapacity,''),
    coalesce(vrt.codsociete, evd.CODE_SOCIETE, 'ODO'),
    coalesce(DataTransmittedClientLEI, clientId_op, NULLIF(bookid,'Washbook_Client')),
    NULL,
    CASE WHEN traderId = 'WebOddo' THEN 'WEB' WHEN ORN IS NOT NULL THEN 'FIX' WHEN ORN IS NULL and clientid is null THEN 'VOIX' END,
    left(CASE WHEN left(clientId_op,7) = 'ROUTAGE' THEN 'CARNET INSTRUCTION' ELSE ORN END,20),
    'ULLINK',
    [Marché/OTC],
    CASE
        WHEN ISNULL(BrokerId,'') IN ('ULBB_PLAT','ODDO','') OR ExDestination IN ('XMON','XEUR') THEN 'VENUE'
        ELSE 'BROKER' END,
    CASE
        WHEN ISNULL(BrokerId,'') IN ('ULBB_PLAT') OR ISNULL(BrokerId,'') IN ('ODDO','') THEN lastMarket
        WHEN ExDestination IN ('XMON','XEUR') THEN ExDestination
        ELSE BrokerId END,
    CASE
        WHEN coalesce(vrt.codsociete ,'',evd.CODE_SOCIETE) = 'OCF' AND isnull(ordercapacity,'') IN ('','liquidityprovider','principal') THEN 'GSM'
        WHEN isnull([ID Utilisateur ordre marché],'') = 'OMSReport' AND left(isnull(clientId_op,''),7) != 'ODDOCAC' THEN 'RTO'
        ELSE left(ordercapacity,20)
    END as TRA_CodTypDistribution,
    RefOrder,
    Ul_id,
    ARM_TVTIC,
    CASE when [owner] ='execution_channel' then orderowner WHEN [owner] like 'OMSReport%' then traderid else owner end,
    CASE WHEN left(clientId_op,7) = 'ODDOCAC' AND convert(varchar, clientSource) = 'I_MARVELSOFT' THEN convert(varchar, instructions_trading) END,
    [Timestamp négociation],
    Altid_Isin,
    Sens,
    null as TRA_IsVenteADecouvert,
    [Quantité exécutée],
    CASE WHEN GIN_CodTypeExpressionCours IN ('02','03','') THEN [Monnaie prix] END as DeviseQte,
    case when [Monnaie prix] = 'GBX' then Prix/100 else Prix end as [Prix],
    case WHEN [Monnaie prix] = 'GBX' THEN 'GBP' ELSE left([Monnaie prix],3) end as [Monnaie prix],
    NULL as MontantNet,
    CASE WHEN DataTransmittedClientLEI IS NOT NULL THEN '969500IHDSNRRY5LDB67' END,
    Counterparty_Id,
    EXECID,
    UL_ID,
    REFORDER,
    BrokerId,
    lastMarket
FROM RD2_V_TRA_ULLINK with(nolock)
outer apply (select * from RD2_V_REF_TIERS where clientId_op = code and code is not null) vrt
left join #EVD_EVOLUTION_DEPOUILLEMENT as evd on Ul_id = evd.REF_UL_ID COLLATE SQL_Latin1_General_CP1_CS_AS and evd.dr = 1
left join (
    select * from (
        select GIN_CodISIN,GIN_CodNature,GIN_CodTypeExpressionCours,
        ROW_NUMBER() OVER(PARTITION BY "GIN_CodISIN" ORDER BY "GIN_CodISIN" desc) AS RowNumberRank 
        from INS_GIN_GeneriqueInstruments
        where GIN_CodNature is not null and GIN_CodISIN is not null
    ) as b 
    where b.RowNumberRank=1
) as GIN on gin.GIN_CodISIN = Altid_Isin
WHERE NOT EXISTS (
    SELECT 1 
    FROM #TRANSACTION_TMP 
    WHERE [TRA_TypSrc] = 'ULL' 
    AND [TRA_IdSrc] = table_id 
    AND [TRA_CodSociete] = coalesce(vrt.codsociete, evd.CODE_SOCIETE, 'ODO')
    -- Add more conditions if needed to uniquely identify records
    -- For example, you might want to include TRA_TradeId or other fields
);
