With A as (With a AS (SELECT DISTINCT(g.customer_id ) customer_id,count(a.account_class) total_accounts
,sum(a.available_balance) Total_Acc_available_balance
,count(distinct g.id) Number_of_goals ,DATEDIFF(year ,i.date_of_birth ,CURRENT_DATE) Current_investor_age
,DATEDIFF(month ,max(g.target_date) ,CURRENT_DATE) Overall_time_left_to_complt_tgt
,(sum(g.target_amount)-sum(current_value))/(NULLIF(DATEDIFF(month ,max(g.target_date),CURRENT_DATE),0)) Overall_Ratio_to_completion
,SUM(current_value)/(sum(g.target_amount)) total_ratio
,sum(current_value)-sum(g.target_amount) Overall_amount_left_to_tgt
,
	   CASE WHEN SUM(g.kickstart_amount) > SUM(current_value) THEN 1
	   		ELSE 0
	   		END AS WIthdraw_from_Total_kickstart,
		CASE WHEN i.gender= 'M'then 1
	 		WHEN i.gender = 'F' then 0
			 ELSE NULL
			 END AS gender


FROM
goals g
LEFT JOIN accounts a
ON g.customer_id=a.customer_id
LEFT join investors i
ON g.customer_id =i.id
WHERE status in (2,5)
GROUP BY 1,i.date_of_birth,i.gender
ORDER by 5 ),

b as (select g.customer_id customer_id_2 ,g.goal_type ,

g.growth_portfolio goal_growth_portfolio,gt.name goal_name,

datediff(month ,g.target_date ,CURRENT_DATE) goal_months_left,datediff(year ,g.target_date ,CURRENT_DATE) goal_years_left,g.step goal_step,
g.kickstart_amount goal_kickstart_amount,g.current_value goal_current_value,g.target_amount goal_target_value,
f.fund_name goal_fund
,current_value/g.target_amount goal_ratio_,
(g.target_amount-current_value)/(NULLIF(DATEDIFF(month ,g.target_date,CURRENT_DATE),0)) goal_Ratio_to_completion

	   ,current_value -g.target_amount goal_amount_left_to_tgt,
	   CASE WHEN g.kickstart_amount > current_value then 1
	   		ELSE 0
	   		END AS WIthdrawn_from_Goal_kickstart,
	   CASE WHEN g.investment_servicing_enabled = TRUE then 1
	   		WHEN g.investment_servicing_enabled =FALSE then 0
	   		ELSE NULL
	   		END AS goal_investment_servicing_enabled,
	   	CASE WHEN g.is_tax_free = TRUE then 1
	   		WHEN g.is_tax_free =FALSE then 0
	   		ELSE NULL
	   		END AS is_goal_tax_free,
   		CASE WHEN g.is_deleted = TRUE then 1
	   		WHEN g.is_deleted =FALSE then 0
	   		ELSE NULL
	   		END AS is_goal_deleted,
	   	CASE WHEN g.status = 5 then 1
	   		WHEN g.status =2 then 0
	   		ELSE NULL
	   		END AS goal_status,
	   	CASE WHEN f.fund_name = 'Old Mutual Core Balanced Fund' then  goal_current_value*POW(1+((8/100::float)/12 ),abs(goal_months_left))
	   	    WHEN f.fund_name = 'Old Mutual Money Market' then  goal_current_value*POW(1+((6/100::float)/12 ),abs(goal_months_left))
	   	    else  goal_current_value*POW(1+((10/100::float)/12 ),abs(goal_months_left))
	   	    end as Compound_int







from
goals g
left join goal_types gt
on g.goal_type =gt.id
left join
fund_names f
on g.fund_id= f.id
where status in (2,5)),


c as (with c_1 as (SELECT DISTINCT t.customer_id, sum(t.amount)
  OVER(
  PARTITION BY t.customer_id, date_trunc('month'::text, t.transaction_date)
  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_amount, t.account_class
   FROM transactions t
  WHERE t.amount > 0::numeric(20,2) AND t.account_class::text = 'Bank'::text AND t.spending_group_id::text <> '4d9c747850610817942e45ab'::text)

  SELECT c_1.customer_id AS id, pg_catalog.percentile_cont(0.5) WITHIN GROUP(
  ORDER BY c_1.total_amount) AS INCOME

   FROM c_1
  GROUP BY c_1.customer_id),


d as (with a as (select  distinct ct.customer_id ,ct.pay_period ,ct.spending_group_description ,sum(Total) Income
	from
	category_totals ct
	where  ct.spending_group_description = 'Income'
	and ct.total>0
	--and ct.pay_period <201905
	group by 1,2,3
	order by 2 desc),

	 b as (select  distinct ct.customer_id ,ct.pay_period ,sum(Total) Expenses
	from
	category_totals ct
	where  ct.spending_group_description in ('Exceptions','Recurring','Day-to-day')
	--and ct.pay_period <201905
	group by 1,2
	order by 1)

	select distinct a.customer_id users_sp_Income ,median(Expenses) over (partition by a.customer_id ) monthly_median_expenses,
	avg(Income) over (partition by a.customer_id ) avg_income_ct_totals
	from a
	join b
	on a.customer_id =b.customer_id
	where a.pay_period= b.pay_period
	and a.Income+b.Expenses>0
	and  a.customer_id in (select g.customer_id
			from
			goals g
			left join goal_types gt
			on g.goal_type =gt.id
			left join
			fund_names f
			on g.fund_id= f.id
			where status in (2,5)
			group by 1)
	group by 1 ,b.expenses,a.income)

	select * from
	a
	inner join b
	on a.customer_id=b.customer_id_2
	left join c
	on c.id =b.customer_id_2
	left join d
	on a.customer_id =d.users_sp_Income),


b as (SELECT  t.customer_id id_2,f.fund_name fund_name_2,avg(amount) Median_Monthly_contribution
FROM transactions t
LEFT JOIN accounts a
ON a.customer_id = t.customer_id
LEFT JOIN  fund_names f
ON a.fund_id = f.id
where a.account_class = 'OMInvestment'
AND t.om_transaction_type = 'Purchase'
AND t.om_status = 'Processed'
AND t.om_transaction_detail in ('Recurring')
AND t.customer_id in (select g.customer_id
			from
			goals g
			left join goal_types gt
			on g.goal_type =gt.id
			left join
			fund_names f
			on g.fund_id= f.id
			where status in (2,5)
			group by 1)
group by 1,2
order by  3 )

select  * ,
CASE WHEN b.fund_name_2 = 'Old Mutual Core Balanced Fund' then  b.Median_Monthly_contribution*(((POW(1+((8/100::float)/12),abs(goal_months_left))-1)/NULLIF(((8/100::float)/12),0)))

WHEN b.fund_name_2 = 'Old Mutual Money Market' then  b.Median_Monthly_contribution*(((POW(1+((6/100::float)/12),abs(goal_months_left))-1)/NULLIF(((6/100::float)/12),0)))
ELSE  b.Median_Monthly_contribution*(((POW(1+((10/100::float)/12),abs(goal_months_left))-1)/NULLIF(((8/100::float)/12),0)))
END AS Fv,(Fv+Compound_int) Fv_Pr
from
a left join b
on a.customer_id =b.id_2
where a.goal_fund =b.fund_name_2
