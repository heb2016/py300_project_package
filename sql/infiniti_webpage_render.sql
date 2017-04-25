/* find dealers web_id, who:
*have page views wihtin the last 2 months; 
*not under Infiniti hierarchi
*/
/*
test: new dealer does not have web site activity, but do have web_id.
select * from dim_securable_items  where upper(site_name) like  '%BREWBAKER INFINITI%'
select * from wca_app.dim_corp_hiers where  dim_securable_item_key= 68478 

*/




SELECT 
            si.web_id
          , sum(pr.total_page_views) 	as Total_Page_Views
          , sum(ws.total_visits)      as Total_Visits
          , sum(ws.total_vdp_views)   as Total_Vdp_Views
         
FROM     wca_app.wevt_page_renders_ma2      AS pr  
left JOIN wca_app.dim_securable_items 		as si ON pr.dim_site_key = si.dim_securable_item_key
left JOIN wca_app.dim_corp_hiers 			as ch ON si.dim_securable_item_key = ch.dim_securable_item_key  
left join wca_app.web_sessions_ma1    as ws on pr.dim_site_key = ws.dim_site_key and pr.dim_year_month_key = ws.dim_year_month_key 

WHERE    1  = 1
         AND lower(si.web_id )   like '%infiniti%'    
         AND pr.dim_year_month_key >= aag_app.get_year_month_key(CURRENT_DATE, '-1')   
         and si.web_id not in 
          (	select si.web_id 
          	from  wca_app.dim_securable_items  si
		 	      JOIN wca_app.dim_corp_hiers ch ON si.dim_securable_item_key = ch.dim_securable_item_key  
	     	    where 1=1
			           and lower(si.web_id )   like '%infiniti%'    
			           and lower(ch.hier_display_name) = 'infiniti'
	          group by si.web_id )
group by 
 	  	    si.web_id
        

order by 
		       web_id