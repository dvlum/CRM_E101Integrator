SELECT	Company as Company,
		Key1 as Idx,
		Key2 as QuoteNum,
		Character01 as CRMCustID
FROM	Ice.UD01
WHERE	CheckBox01 = 0 and Key3 = 'E101QuoteData'