select x.stats_date 统计日期,
       x.region_name 区,
	   xy.staffs 月初抗标人数,
	   xz.new_req 新线索触碰量,
	   y.new_keys 获取新线索量,
	   z.new_plan_num 新线索试听邀约量,
	   z.new_trial_exp 新线索体验课出席量,
	   z.new_trial_num 新线索试听出席量,
	   z.new_trial_deal 新线索试听成单量,
	   xx.new_deal_num 新线索总成单量,
	   xx.new_deal_amount 新线索总成单额,
	   xz.all_oc_req 大盘oc触碰量,
	   y.all_oc_keys 大盘oc获取量,
	   z.all_oc_plan_num 大盘oc试听邀约量,
	   z.all_oc_trial_exp 大盘oc体验课出席量,
	   z.all_oc_trial_num 大盘oc试听出席量,
	   z.all_oc_trial_deal 大盘oc试听成单量,
	   xx.all_oc_deal_num 大盘oc总成单量,
	   xx.all_oc_deal_amount 大盘oc总成单额,
	   y.new_rec_keys 获取转介绍线索量,
	   z.new_rec_plan_num 新转介绍线索试听邀约量,
	   z.new_rec_trial_exp 新转介绍体验课出席量,
	   z.new_rec_trial_num 新转介绍试听出席量,
	   z.new_rec_trial_deal 新转介绍试听成单量,
	   xx.new_rec_deal_num 新转介绍总成单量,
	   xx.new_rec_deal_amount 新转介绍总成单额
	   
	   
			
from (			
			
			select  date_sub(curdate(),interval 1 day) stats_date,
					concat(ifnull(cdme.city,''),ifnull(cdme.branch,''),ifnull(cdme.center,''),ifnull(cdme.region,'')) region_name
			from bidata.charlie_dept_month_end cdme
			where cdme.stats_date = curdate()
				  and cdme.class = '销售'
				  and cdme.date >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
			group by stats_date, region_name
			having region_name like '上海_区'
				   or region_name = '销售考核'
				   ) as x

left join (
             select 
                      date_sub(curdate(),interval 1 day) stats_date,
	                  concat(ifnull(cdme.city,''),ifnull(cdme.branch,''),ifnull(cdme.center,''),ifnull(cdme.region,'')) region_name,
	                  count(distinct intention_id) all_keys, -- 获取的总线索量
                      count(distinct case when s.submit_time >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
			                              then intention_id end) new_keys, -- 获取的新线索量
	                  count(distinct case when s.submit_time < date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
			                              then intention_id end) all_oc_keys,  -- 获取的大盘oc线索量
	                  count(distinct case when s.submit_time >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
			                                   and (s.coil_in in (13,22) or s.know_origin in (56,71,22,24,25,41))
						                  then intention_id end) new_rec_keys	 -- 获取新转介绍新线索量										 
             from
                   (
                       select tpel.track_userid, tpel.intention_id, tpel.into_pool_date, ui.name distri_user
                       from hfjydb.tms_pool_exchange_log tpel
                       inner join bidata.charlie_dept_month_end cdme on cdme.user_id = tpel.track_userid
	                              and cdme.stats_date = curdate() and cdme.class = '销售'
	                              and cdme.date >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
                       left join hfjydb.view_user_info ui on ui.user_id = tpel.create_userid
                       where tpel.into_pool_date >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
	                         and tpel.into_pool_date < curdate()
                       union all
                       select tnn.user_id as track_userid, tnn.student_intention_id as intention_id, tnn.create_time as into_pool_date, 'OC分配账号' distri_user
                       from hfjydb.tms_new_name_get_log tnn
                       inner join bidata.charlie_dept_month_end cdme on cdme.user_id = tnn.user_id
	                              and cdme.stats_date = curdate() and cdme.class = '销售'
	                              and cdme.date >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
                       where tnn.create_time >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
	                         and tnn.create_time < curdate()
	                         and student_intention_id <> 0
							 ) as a							 

             left join hfjydb.view_student s on s.student_intention_id = a.intention_id
             left join bidata.charlie_dept_month_end cdme on cdme.user_id = a.track_userid
                       and cdme.stats_date = curdate()
		               and cdme.date >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')

             group by region_name, stats_date
			 ) as y on y.region_name = x.region_name and y.stats_date = x.stats_date

left join (
             select
                      date_sub(curdate(),interval 1 day) stats_date,
		              b.region_name,
		              count(distinct b.student_intention_id) all_plan_num, -- 总试听邀约量
                      count(distinct(case when b.key_attr = 'new' then b.student_intention_id else null end)) new_plan_num, -- 新线索试听邀约量
		              count(distinct(case when b.key_attr = 'all_oc' then b.student_intention_id else null end)) all_oc_plan_num, -- 大盘oc线索试听邀约量,
                      count(distinct(case when b.is_recommend = 1 and b.key_attr = 'new' then b.student_intention_id else null end)) new_rec_plan_num, -- 转介绍线索试听邀约量
		              count(distinct(case when b.is_trial = 1 then b.student_intention_id else null end)) all_trial_num, -- 总试听出席量
		              count(distinct(case when b.key_attr = 'new' and b.is_trial = 1 then b.student_intention_id else null end)) new_trial_num, -- 新线索试听出席量
                      count(distinct(case when b.key_attr = 'all_oc' and b.is_trial = 1 then b.student_intention_id else null end)) all_oc_trial_num, -- 大盘oc试听出席量
                      count(distinct(case when b.key_attr = 'new' and b.is_recommend = 1 and b.is_trial = 1 then b.student_intention_id else null end)) new_rec_trial_num, -- 转介绍新线索试听出席量
	                  count(distinct(case when b.is_trial_deal = 1 then b.student_intention_id else null end)) all_trial_deal, -- 总试听成单量
		              count(distinct(case when b.key_attr = 'new' and b.is_trial_deal = 1 then b.student_intention_id else null end)) new_trial_deal, -- 新线索试听成单量
		              count(distinct(case when b.key_attr = 'all_oc' and b.is_trial_deal = 1 then b.student_intention_id else null end)) all_oc_trial_deal, -- 大盘oc线索试听成单量
		              count(distinct(case when b.key_attr = 'new' and b.is_trial_deal = 1 and b.is_recommend = 1 then b.student_intention_id else null end)) new_rec_trial_deal, -- 转介绍新线索试听成单量
		              count(distinct(case when b.is_trial_exp = 1 then b.student_intention_id else null end)) all_trial_exp, -- 总体验课数量
		              count(distinct(case when b.key_attr = 'new' and b.is_trial_exp = 1 then b.student_intention_id else null end)) new_trial_exp, -- 新线索体验课数量
		              count(distinct(case when b.key_attr = 'all_oc' and b.is_trial_exp = 1 then b.student_intention_id else null end)) all_oc_trial_exp, -- 大盘oc线索体验课数量
		              count(distinct(case when b.key_attr = 'new' and b.is_trial_exp = 1 and b.is_recommend = 1 then b.student_intention_id else null end)) new_rec_trial_exp -- 转介绍新线索体验课数量
		
             from(    
                    select
							   lpo.apply_user_id, lpo.student_intention_id, s.student_no,
							   concat(ifnull(cdme.city,''),ifnull(cdme.branch,''),ifnull(cdme.center,''),ifnull(cdme.region,'')) region_name,
							   (case when lp.status in (3,5) and lp.solve_status <> 6 then 1 else 0 end) is_trial,
							   (case when s.submit_time >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01') then 'new' else 'all_oc' end) key_attr,
							   (case when s.coil_in in (13,22) or s.know_origin in (56,71,22,24,25,41)  then 1 else 0 end) is_recommend,
							   (case when aa.real_pay_amount > 0 then 1 else 0 end) is_trial_deal,
							   (case when lp.student_id in (select distinct student_id
															from hfjydb.lesson_plan  
															where lesson_type=3 and status = 3 and solve_status <> 6 
																  and adjust_start_time >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
																  and adjust_start_time < curdate()
																  )
				                     then 1 else 0 end) is_trial_exp			  			  
					from hfjydb.lesson_plan_order lpo
					left join hfjydb.lesson_relation lr on lpo.order_id = lr.order_id
					left join hfjydb.lesson_plan lp on lr.plan_id = lp.lesson_plan_id
					left join hfjydb.view_student s on s.student_intention_id = lpo.student_intention_id
					inner join bidata.charlie_dept_month_end cdme on cdme.user_id = lpo.apply_user_id 
							   and cdme.stats_date = curdate() and cdme.class = '销售'
							   and cdme.date >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
		     left join (
							select min(tcp.pay_date) min_date,
								   tc.contract_id,
								   tc.student_intention_id,
								   sum(tcp.sum/100) real_pay_amount,
								   (tc.sum-666) * 10 contract_amount,
								   tcp.submit_user_id,
								   concat(ifnull(cdme.city,''),ifnull(cdme.branch,''),ifnull(cdme.center,''),ifnull(cdme.region,'')) region_name
							from hfjydb.view_tms_contract_payment tcp
							left join hfjydb.view_tms_contract tc on tc.contract_id = tcp.contract_id
							inner join bidata.charlie_dept_month_end cdme on cdme.user_id = tcp.submit_user_id
									   and cdme.stats_date = curdate() and cdme.class = '销售'
									   and cdme.date >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
							where tcp.pay_status in (2, 4)
								  and tc.status <> 8
							group by tc.contract_id, region_name
							having max(tcp.pay_date) >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
								   and max(tcp.pay_date) < curdate()
								   and real_pay_amount >= contract_amount
								   ) as aa on aa.student_intention_id = lpo.student_intention_id
											  and aa.region_name = 	concat(ifnull(cdme.city,''),ifnull(cdme.branch,''),ifnull(cdme.center,''),ifnull(cdme.region,''))				   
             where lp.lesson_type = 2
				   and lp.adjust_start_time >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
				   and lp.adjust_start_time < curdate()
				   and s.account_type = 1
				   ) as b
             group by region_name, stats_date
			 ) as z on z.region_name = x.region_name and z.stats_date = x.stats_date


left join (
             select  
                    date_sub(curdate(),interval 1 day) stats_date,
					a.region_name,
					count(a.contract_id) as all_deal_num,   -- 总成单量
					sum(a.real_pay_sum) as all_deal_amount,   -- 总成单额
					count(case when a.key_attr = 'new' then a.contract_id else null end) as new_deal_num,   -- 新线索成单量
					sum(case when a.key_attr = 'new' then a.real_pay_sum else 0 end) as new_deal_amount,   -- 新线索成单额
					count(case when a.key_attr = 'all_oc' then a.contract_id else null end) as all_oc_deal_num,   -- 大盘oc线索成单量
					sum(case when a.key_attr = 'all_oc' then a.real_pay_sum else 0 end) as all_oc_deal_amount,   -- 大盘oc线索成单额
					count(case when a.key_attr = 'new' and (a.coil_in in (13,22) or a.know_origin in (56,71,22,24,25,41))
							   then a.contract_id else null end) as new_rec_deal_num,   -- 新线索转介绍成单量
					sum(case when a.key_attr = 'new' and (a.coil_in in (13,22) or a.know_origin in (56,71,22,24,25,41))
							 then a.real_pay_sum else 0 end) as new_rec_deal_amount   -- 新线索转介绍成单额

             from(
					select
							min(tcp.pay_date) min_pay_date,
							max(tcp.pay_date) last_pay_date,
							tcp.contract_id,  
							s.student_no,
							s.student_intention_id,
							s.submit_time,
							concat(ifnull(cdme.city,''),ifnull(cdme.branch,''),ifnull(cdme.center,''),ifnull(cdme.region,'')) region_name,
							case when s.submit_time >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01') then 'new' else 'all_oc' end key_attr,
							s.coil_in,
							s.know_origin,
							sum(tcp.sum)/100 real_pay_sum, 
							ui.name,
							ui.job_number,
							(tc.sum-666)*10 contract_amount
					from hfjydb.view_tms_contract_payment tcp
					left join hfjydb.view_tms_contract tc on tcp.contract_id  = tc.contract_id
					left join hfjydb.view_student s on s.student_intention_id = tc.student_intention_id
					left join hfjydb.view_user_info ui on ui.user_id = tc.submit_user_id
					inner join bidata.charlie_dept_month_end cdme on cdme.user_id = tcp.submit_user_id
							   and cdme.stats_date = curdate() and cdme.class = '销售'
							   and cdme.date >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
					where tcp.pay_status in (2,4)
						  and ui.account_type = 1
					group by tcp.contract_id
					having real_pay_sum >= contract_amount
						   and last_pay_date >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
						   and last_pay_date < curdate()
						   ) as a
             group by a.region_name, stats_date
			 ) as xx on xx.region_name = x.region_name and xx.stats_date = x.stats_date


left join (
              select date_sub(curdate(),interval 1 day) stats_date,
                     region_name,
			         sum(number) as staffs

              from(
		              select distinct cdme.department_name,
			                 concat(ifnull(cdme.city,''),ifnull(cdme.branch,''),ifnull(cdme.center,''),ifnull(cdme.region,'')) region_name,
							 st.number
		              from bidata.sales_tab st
		              inner join bidata.charlie_dept_month_end cdme on cdme.department_name = st.group_name
				                 and cdme.stats_date = curdate() and cdme.class = '销售'
				                 and cdme.date >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
								 ) as t
               group by region_name,stats_date
			   ) as xy on xy.region_name = x.region_name and xy.stats_date = x.stats_date

left join (
             select date_sub(curdate(), interval 1 day) stats_date,
                    concat(ifnull(cdme.city,''),ifnull(cdme.branch,''),ifnull(cdme.center,''),ifnull(cdme.region,'')) region_name,
                    count(distinct case when s.submit_time >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01') 
                                        then ss.student_intention_id else null end) as new_req,
	                count(distinct case when s.submit_time < date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01') 
                                        then ss.student_intention_id else null end) as all_oc_req
             from hfjydb.ss_collection_sale_roster_action ss
             left join bidata.charlie_dept_month_end cdme on cdme.user_id = ss.user_id
                       and cdme.stats_date = curdate() and cdme.class = '销售'
		               and cdme.date >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01') 
             left join hfjydb.view_student s on s.student_intention_id = ss.student_intention_id
             where ss.view_time >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01') 
                   and ss.view_time < curdate()
             group by region_name, stats_date
			 ) as xz on xz.region_name = x.region_name and xz.stats_date = x.stats_date;




select x.stats_date 统计日期,
       x.region_name 区,
	   y.total_performance 总流水业绩,
	   y.new_performance 新线索流水业绩,
	   y.rate 月应完标率,
	   y.all_oc_performance 大盘oc线索流水业绩,
	   y.new_rec_performance 转介绍新线索流水业绩,
	   z.achievement 业绩指标,
	   z.staffs 月初抗标人数,
	   xx.total_performance_splm 上月同期流水业绩
	   


from (

			select  date_sub(curdate(),interval 1 day) stats_date,
					concat(ifnull(cdme.city,''),ifnull(cdme.branch,''),ifnull(cdme.center,''),ifnull(cdme.region,'')) region_name
			from bidata.charlie_dept_month_end cdme
			where cdme.stats_date = curdate()
				  and cdme.class = '销售'
				  and cdme.date >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
			group by stats_date, region_name
			having region_name like '上海_区'
				   or region_name = '销售考核'
				   ) as x

left join (
              select
                      date_sub(curdate(),interval 1 day) stats_date,
                      concat(ifnull(cdme.city,''),ifnull(cdme.branch,''),ifnull(cdme.center,''),ifnull(cdme.region,'')) region_name,
                      sum(tcp.sum/100) total_performance,   -- 总流水业绩
					  dayofmonth(date_sub(curdate(),interval 1 day))/dayofmonth(last_day(date_sub(curdate(),interval 1 day))) rate, 
	                  sum(case when s.submit_time >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
                               then tcp.sum/100 else 0 end) new_performance,    -- 新线索流水业绩
	                  sum(case when s.submit_time < date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
                               then tcp.sum/100 else 0 end) all_oc_performance,    -- 大盘oc线索流水业绩
                      sum(case when s.submit_time >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
		                            and (s.coil_in in (13,22) or s.know_origin in (56,71,22,24,25,41))
                               then tcp.sum/100 else 0 end) new_rec_performance    -- 转介绍新线索流水业绩 
              from bidata.charlie_dept_month_end cdme
              left join hfjydb.view_tms_contract tc on tc.submit_user_id = cdme.user_id
                        and tc.status <> 8
              left join hfjydb.view_student s on tc.student_no =s.student_no
              left join hfjydb.view_tms_contract_payment tcp on tcp.contract_id = tc.contract_id
                        and tcp.pay_date >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
		                and tcp.pay_date < curdate() and tcp.pay_status in (2,4)
              where cdme.class = '销售'
                    and cdme.stats_date = curdate()
                    and cdme.date >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
              group by region_name, stats_date
			  ) as y on x.region_name = y.region_name and x.stats_date = y.stats_date

left join (
               select date_sub(curdate(),interval 1 day) stats_date,
               region_name,
               sum(achievement) as achievement,
			   sum(number) as staffs

               from(
		              select distinct cdme.department_name,
			                 concat(ifnull(cdme.city,''),ifnull(cdme.branch,''),ifnull(cdme.center,''),ifnull(cdme.region,'')) region_name,
			                 st.achievement,
							 st.number

		              from bidata.sales_tab st
		              inner join bidata.charlie_dept_month_end cdme on cdme.department_name = st.group_name
				                 and cdme.stats_date = curdate() and cdme.class = '销售'
				                 and cdme.date >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
								 ) as t
               group by region_name,stats_date
			   ) as z on z.region_name = x.region_name and z.stats_date = x.stats_date

left join (
                select
                        date_sub(curdate(),interval 1 day) stats_date,
                        concat(ifnull(cdme.city,''),ifnull(cdme.branch,''),ifnull(cdme.center,''),ifnull(cdme.region,'')) region_name,
                        sum(tcp.sum/100) total_performance_splm   -- 上月同期总流水业绩    
                from bidata.charlie_dept_month_end cdme
                left join hfjydb.view_tms_contract tc on tc.submit_user_id = cdme.user_id
                          and tc.status <> 8
                left join hfjydb.view_student s on tc.student_no =s.student_no
                left join hfjydb.view_tms_contract_payment tcp on tcp.contract_id = tc.contract_id
                          and tcp.pay_date >= date_sub(date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01'),interval 1 month)
		                  and tcp.pay_date < date_sub(curdate(),interval 1 month)
		                  and tcp.pay_status in (2,4)
                where cdme.class = '销售' and cdme.stats_date = curdate()
                      and cdme.date >= date_format(date_sub(curdate(),interval 1 day),'%Y-%m-01')
                group by region_name, stats_date
				) as xx on xx.region_name = x.region_name and xx.stats_date = x.stats_date;