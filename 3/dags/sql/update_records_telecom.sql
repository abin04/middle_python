UPDATE telecom_companies SET name_corp = substring(name_corp from Strpos(name_corp,'"')+1 for Length(name_corp)-Strpos(name_corp,'"')-1) WHERE Strpos(name_corp,'"')>0; 
