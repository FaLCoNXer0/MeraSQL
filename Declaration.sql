ALTER FUNCTION udtf_Report_DeclarationProfile_082923
(
  @CompanyPK as uniqueidentifier,
  @OrderedStorageClassCodesList AS varchar(1000)
)
RETURNS TABLE
AS  
RETURN
SELECT     
DISTINCT
	CASE     
		WHEN JobHeader.JH_PK IS NOT NULL THEN 'Y'    
	END AS HeaderExists,    
	JE_DeclarationReference		AS DecRef,    
	CASE 
		WHEN JE_JS IS NULL THEN 'B' 
		ELSE 'S' 
	END AS JobType,    
	JE_TransportMode			AS Transport,    
	JE_MessageType				AS Type,    
	JE_ContainerMode			AS Mode,    
	JE_RL_NKOrigin				AS Origin,    
	JE_RL_NKFinalDestination		AS Dest,    
	case when JE_IsCancelled = 1 then 'Y' else 'N' END					AS IsCancelled,  
	JE_RL_NKPortOfLoading       AS 'Load',    
	JE_RL_NKPortOfArrival       AS Arrival,    
	JE_RL_NKPortOfFirstArrival	AS FirstArrival,    
	JE_DateAtOrigin				AS OriginETD,    
	JE_DateAtFinalDestination   AS DestETA,    
	JE_ExportDate				AS LoadATD,    
	JE_DateOfArrival			AS ArrivalATA,    
	JE_DateOfFirstArrival       AS FirstArrivalATA,    
	JE_SystemCreateTimeUtc         AS DeclarationDate,    
	JE_OH_Supplier				AS Supplier,    
	Supplier.OH_Code			AS SupplierCode,    
	Supplier.OH_FullName		AS SupplierName,    
	JE_OH_Importer				AS Importer,    
	Importer.OH_Code			AS ImporterCode,    
	Importer.OH_FullName        AS ImporterName,    
	JE_HouseBill				AS HouseBill,    
	JE_MasterBill				AS MasterBill,    
	JE_VesselName				AS Vessel,    
	JE_VoyageFlightNo			AS Voyage,    
	JE_TotalWeight				AS Weight,    
	JE_TotalWeightUnit			AS WeightUnit,    
	JE_TotalVolume				AS Volume,    
	JE_TotalVolumeUnit			AS VolumeUnit,    
	JE_TotalNoOfPacks			AS Packs,    
	JE_TotalNoOfPacksPackType   AS PacksType,    
	JE_GB						AS DeclarationBranchPK,    
	DeclarationBranch.GB_Code   AS DeclarationBranch,    
	ISNULL(Containers.TotalTEU, 0)		AS TEU,    
	ISNULL(Containers.DecContCount, 0)	AS ContainerCount,    
	JobHeaderBranch.GB_Code         AS Branch,    
	JobHeaderDept.GE_Code			AS Dept,    
	Jobheader.JH_GB					as BranchPK,     
	Jobheader.JH_GE					as DepartmentPK,     
	LocalClient.OH_PK				as LocalClientPK,     
	LocalClient.OH_Code				AS LocalClient,     
	LocalClient.OH_FullName         as LocalClientName,    
	JH_Status						AS JobStatus,
	JH_SystemCreateTimeUtc				AS JobCreatedDate,     
	SalesRep.GS_PK					as SalesRepPK, 
	SalesRep.GS_FullName			AS SalesRepName,    
	Operator.GS_PK					as OperatorPK, 
	Operator.GS_FullName			AS OperatorName,   
	AL_LineAmount,
	WIPAmount,
	CSTAmount,
	ACRAmount,
	REVAmount,   
	CountOther,
	Count1, Count2, Count3,     
	Count4, Count5, Count6,    
	Count7, Count8, Count9,     
	Count10, Count11, Count12,     
	Count13, Count14, Count15,
	JE_JZ_RowCount, 
	JE_JL_RowCount,
	SG_OutwardTransportMode,
    SG_US_NKPlaceOfCargoRelease,
    SG_US_NKPlaceOfReceipt,
    OutShipLine.OH_Code AS SG_OH_OutwardShippingLine,
    SG_OutwardMAWB,
    SG_OutwardHAWB,
    SG_OutwardVoyageFlightNo,
	SG_OutwardVesselName,
	DelayCode = dbo.GetCustomField (JE_PK, 'Delay Reason Code'),
	DeliveryNo = dbo.GetCustomField (JE_PK, 'Delivery No.'),
	

--Added By Abdur--
 OrgHeaderForwarder.OH_PK AS OH_PKReceivingAgent,
 OrgHeaderForwarder.OH_Code AS OH_CodeReceivingAgent,
 OrgHeaderForwarder.OH_FullName AS OH_FullNameReceivingAgent,
 ForwarderCode = JE_OH_Forwarder,
 ContainerNumbers = (SELECT Value FROM dbo.ctfn_JobShipmentContainerNumbers(JS_PK)),
 HBL# = JE_HouseBill,
 ContainerSealNumber		= JC_SealNum,
 BookingDate = JE_SystemCreateTimeUtc,
 HandOverDate = JW_TerminalReceivalCommences,
 -- ContStatus            = JC_ContainerStatus,
 -- ContQuality           = JC_ContainerQuality,
 ConSize			   = JC_RC,
 OriginDate = JE_DateAtOrigin,
 DischargeDate = JE_DateOfArrival,
 FirstPort = JE_RL_NKPortOfArrival,
 EntryNum = CE_EntryNum,
 EntryDate = CE_IssueDate,
 ReleaseDate = JE_EntryAuthorisationDate,
 PostedTime = P9_ActualDate,
 ArrivalDestDate = JE_EntryDate,
 ActualDlvDate = JP_DeliveryCartageCompleted

FROM 
	dbo.JobDeclaration
	
 LEFT JOIN JobHeader ON JH_ParentID = JE_PK AND JH_ParentTableCode = 'JE' 
 LEFT JOIN dbo.GlbCompany ON JH_GC = GC_PK
 LEFT JOIN ProcessTasks ON P9_ParentID = JE_PK 
 JOIN dbo.CusContainer ON CO_JE = JE_PK
 JOIN dbo.JobContainer ON JobContainer.JC_PK = CO_JC
 LEFT JOIN dbo.JobConsol AS Consol ON Consol.JK_PK = JC_JK
 LEFT JOIN dbo.JobConsolTransport ON  JW_ParentGUID = Consol.JK_PK 
 LEFT JOIN dbo.JobDocsAndCartage ON JP_ParentID = JE_PK
 JOIN dbo.CusEntryHeader ON CH_JE = JE_PK
 --JOIN dbo.CusEntryNum ON CE_ParentID = CH_PK
 LEFT JOIN dbo.CusEntryNum ON CE_ParentTable = 'JobDeclaration' AND CE_ParentID = JE_PK
 LEFT JOIN dbo.OrgHeader OrgHeaderForwarder  ON JE_OH_Forwarder = OrgHeaderForwarder.OH_PK 

--Added By Abdur--

	INNER JOIN dbo.GlbBranch DeclarationBranch
		ON 
			JE_GB = GB_PK AND 
			GB_GC = @CompanyPK
	LEFT JOIN dbo.cvw_DeclarationCommercialInvoiceStats on JobDeclaration.JE_PK = cvw_DeclarationCommercialInvoiceStats.JE_PK 
	LEFT JOIN dbo.OrgHeader AS Supplier ON JE_OH_Supplier = Supplier.OH_PK 
	LEFT JOIN dbo.OrgHeader AS Importer ON JE_OH_Importer = Importer.OH_PK 
	LEFT JOIN dbo.FCLDeclarationContainers(@OrderedStorageClassCodesList) AS Containers ON Containers.DecPK = JobDeclaration.JE_PK 
	LEFT JOIN dbo.JobShipment ON JE_JS = JS_PK 
--LEFT JOIN dbo.JobHeader ON JH_ParentID in( JS_PK, JobDeclaration.JE_PK ) AND JH_GC = DeclarationBranch.GB_GC AND JH_IsActive = 1
	LEFT JOIN  
	(
		SELECT
			JH_PK
			,SUM(AL_LineAmount) AS AL_LineAmount
			,SUM(WIPAmount) AS WIPAmount
			,SUM(ACRAmount) AS ACRAmount
			,SUM(CSTAmount) AS CSTAmount
			,SUM(REVAmount) AS REVAmount
		FROM
			dbo.JobHeader 
			INNER JOIN dbo.vw_ClassifiedTransactionLineAmountsIncludingCancelled 
				ON  
					JH_PK = AL_JH
					AND
					JH_GC = @CompanyPK
					AND
					(JH_ParentTableCode = 'JE' OR JH_ParentTableCode = 'JS')
					AND
					(
						AL_PostDate IS NOT NULL
						AND  
						(AL_ReverseDate IS NULL OR AL_LineType IN ('REV','CST'))
					)   
		GROUP BY
			JH_PK
	) AS HeaderAmounts
		ON  
			JobHeader.JH_PK = HeaderAmounts.JH_PK    
	LEFT JOIN dbo.GLBDepartment JobHeaderDept on JH_GE = JobHeaderDept.GE_PK 
	LEFT JOIN dbo.GLBBranch JobHeaderBranch on JH_GB = JobHeaderBranch.GB_PK 
	LEFT JOIN dbo.OrgAddress As LocalChargesAddress ON JH_OA_LocalChargesAddr = LocalChargesAddress.OA_PK 
	LEFT JOIN dbo.OrgHeader AS LocalClient ON LocalChargesAddress.OA_OH = LocalClient.OH_PK 
	LEFT JOIN dbo.GLBStaff SalesRep on JH_GS_NKRepSales = SalesRep.GS_Code 
	LEFT JOIN dbo.GLBStaff Operator on JH_GS_NKRepOps = Operator.GS_Code 
	LEFT JOIN dbo.JobDocAddress AS OutShipLineDocAddress ON OutShipLineDocAddress.E2_ParentID = JobDeclaration.JE_PK AND OutShipLineDocAddress.E2_AddressType = 'OCT' AND OutShipLineDocAddress.E2_AddressSequence = 0
	LEFT JOIN dbo.OrgAddress AS OutShipLineAddress ON OutShipLineAddress.OA_PK = OutShipLineDocAddress.E2_OA_Address
	LEFT JOIN dbo.OrgHeader AS OutShipLine  ON OutShipLine.OH_PK = OutShipLineAddress.OA_OH
	OUTER APPLY dbo.ctfn_SingaporeJobDeclarationAddInfoValues(JobDeclaration.JE_PK, @CompanyPK)