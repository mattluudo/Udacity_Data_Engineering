WITH
  --Table to lookup market and submarket of each cell
  --Note: Add and remove markets/submarket as needed to speed up query
  --DON'T CHANGE DATE FOR THIS TABLE
  market AS (
    SELECT DISTINCT
      MARKETCLUSTER AS MARKET,
      MARKET AS SUBMARKET,
      EUTRANCELL_NAME

    FROM PDW_MOBILITYSCORECARD_VIEWS.LTE_RTB_SCORECARD_DY
    WHERE 
      EUCELLVENDORLIST = 'E' AND MARKET <> '*' AND MARKET <> '*COMMERCIAL' 
	  AND MARKET <> '-' AND MARKET NOT LIKE '%-PART' AND EUTRANCELL_NAME <> '*' 
	  AND DATETIMELOCAL >= '2020-02-01'  
  )

SELECT 
  TRUNC(cell.DATETIME) AS DATETIME,
  market.MARKET,
  market.SUBMARKET,
  'E' AS VENDOR,
  market.EUTRANCELL_NAME AS EUTRANCELL, 
  
  --RRC CONNECTION 
  PMRRCCONNLEVSUM AS AVG_RRC_CONN_NUM,
  NULLIFZERO(PMRRCCONNLEVSAMP) AS AVG_RRC_CONN_DEN,
  PMRRCCONNLEVSUM/NULLIFZERO(PMRRCCONNLEVSAMP) AS AVG_RRC_CONN,
  
  --TOTAL TP
  PMPDCPVOLDLDRB - pmPdcpVolDlDrbLastTTI AS DL_DRB_TP_NUM,
  NULLIFZERO(pmUeThpTimeDl)/1000 AS DL_DRB_TP_DEN,
  1000*(PMPDCPVOLDLDRB - pmPdcpVolDlDrbLastTTI)/NULLIFZERO(pmUeThpTimeDl) AS DL_DRB_TP,
  
  --QCI TP
  cell4.PMPDCPVOLDLDRBQCI6 - cell4.PMPDCPVOLDLDRBLASTTTIQCI6 AS QCI6_DL_TPUT_NUM,
  cell4.PMPDCPVOLDLDRBQCI7 - cell4.PMPDCPVOLDLDRBLASTTTIQCI7 AS QCI7_DL_TPUT_NUM,
  cell4.PMPDCPVOLDLDRBQCI8 - cell4.PMPDCPVOLDLDRBLASTTTIQCI8 AS QCI8_DL_TPUT_NUM,
  cell4.PMPDCPVOLDLDRBQCI9 - cell4.PMPDCPVOLDLDRBLASTTTIQCI9 AS QCI9_DL_TPUT_NUM,
  
  cell4.PMDRBTHPTIMEDLQCI6/1000 AS QCI6_DL_TPUT_DEN,
  cell4.PMDRBTHPTIMEDLQCI7/1000 AS QCI7_DL_TPUT_DEN,
  cell4.PMDRBTHPTIMEDLQCI8/1000 AS QCI8_DL_TPUT_DEN,
  cell4.PMDRBTHPTIMEDLQCI9/1000 AS QCI9_DL_TPUT_DEN,
  
  1000*(cell4.PMPDCPVOLDLDRBQCI6 - cell4.PMPDCPVOLDLDRBLASTTTIQCI6)/NULLIFZERO(cell4.PMDRBTHPTIMEDLQCI6) AS DL_DRB_QCI6_TP,
  1000*(cell4.PMPDCPVOLDLDRBQCI7 - cell4.PMPDCPVOLDLDRBLASTTTIQCI7)/NULLIFZERO(cell4.PMDRBTHPTIMEDLQCI7) AS DL_DRB_QCI7_TP,
  1000*(cell4.PMPDCPVOLDLDRBQCI8 - cell4.PMPDCPVOLDLDRBLASTTTIQCI8)/NULLIFZERO(cell4.PMDRBTHPTIMEDLQCI8) AS DL_DRB_QCI8_TP,
  1000*(cell4.PMPDCPVOLDLDRBQCI9 - cell4.PMPDCPVOLDLDRBLASTTTIQCI9)/NULLIFZERO(cell4.PMDRBTHPTIMEDLQCI9) AS DL_DRB_QCI9_TP,
  
  --ERAB CONNECTIONS
  ZEROIFNULL(cell3.PMERABESTABSUCCINITQCI6) + ZEROIFNULL(cell3.PMERABESTABSUCCADDEDQCI6) AS QCI6_ERAB_SETUP_SUCC,
  ZEROIFNULL(cell3.PMERABESTABSUCCINITQCI7) + ZEROIFNULL(cell3.PMERABESTABSUCCADDEDQCI7) AS QCI7_ERAB_SETUP_SUCC,
  ZEROIFNULL(cell3.PMERABESTABSUCCINITQCI8) + ZEROIFNULL(cell3.PMERABESTABSUCCADDEDQCI8) AS QCI8_ERAB_SETUP_SUCC,
  ZEROIFNULL(cell3.PMERABESTABSUCCINITQCI9) + ZEROIFNULL(cell3.PMERABESTABSUCCADDEDQCI9) AS QCI9_ERAB_SETUP_SUCC
  
FROM 
  PDW_ERIEUTRAN_VIEWS.V_RBS_EUTRANCELLFDD_DY cell,
  PDW_ERIEUTRAN_VIEWS.V_RBS_EUTRANCELLFDD3_DY cell3,
  PDW_ERIEUTRAN_VIEWS.V_RBS_EUTRANCELLFDD4_DY cell4,
  market
  
WHERE 
  cell.DATETIME = cell3.DATETIME AND
  cell.ENODEB = cell3.ENODEB AND
  cell.EUTRANCELLFDD = cell3.EUTRANCELLFDD AND
  
  cell.DATETIME = cell4.DATETIME AND
  cell.ENODEB = cell4.ENODEB AND
  cell.EUTRANCELLFDD = cell4.EUTRANCELLFDD AND
  
  cell.EUTRANCELLFDD = market.EUTRANCELL_NAME AND
  
  cell.DATETIME >= '{start_date}' AND cell.DATETIME < '{end_date}' AND
  cell3.DATETIME >= '{start_date}' AND cell3.DATETIME < '{end_date}' AND
  cell4.DATETIME >= '{start_date}' AND cell4.DATETIME < '{end_date}' AND
  
  PMPDCPVOLDLDRB - pmPdcpVolDlDrbLastTTI > 0 AND
  pmUeThpTimeDl > 0

ORDER BY
  MARKET, 
  SUBMARKET, 
  DATETIME, 
  EUTRANCELL_NAME
 

  
