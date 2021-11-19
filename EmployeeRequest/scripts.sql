/*Average by person */
select 
et.full_name, 
count(completed_at) as total_transfers, 
sum(tt.net_amount) as net_transfer, 
round(avg(tt.net_amount)::numeric,2)as transfer_average, sum(tt.fee) as fees from transactions_data_tab tdt 

/*Less than 3 */
select * from (select et.full_name, count(completed_at) as total_transfers, 
sum(tt.net_amount) as net_transfer, round(avg(tt.net_amount)::numeric,2)as transfer_average, 
sum(tt.fee) as fees from transactions_data_tab tt 
inner join employees_data_table et on
tt.employee_id =et.employee_id 
where tt.completed_at is not null
group by et.full_name 
) as t where t.total_transfers < 3

/*Most recnt */
select edt.full_name, coalesce (tdt.net_amount,0) from
employees_data_table as edt 
left join transactions_data_tab tdt
on edt.employee_id = tdt.employee_id 
left join (select tdt.employee_id as employee_id , max(tdt.completed_at) as completed_at
from transactions_data_tab tdt
group by tdt.employee_id) as mr
on tdt.employee_id = mr.employee_id
and tdt.completed_at = mr.completed_at
where mr.completed_at = tdt.completed_at or tdt.completed_at is null
