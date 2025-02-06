--  data cleaning 

select * from layoffs ;


-- 1 : remove duplicates 

select * from layoffs_staging;


create table layoffs_staging as
(select * from layoffs);

select * from layoffs_staging;

with cte_1 as (
select * , row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num from layoffs_staging)
select * from cte_1 ;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int(11) DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int(11) DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


insert into layoffs_staging2
select * , row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num from layoffs_staging;


delete 
from layoffs_staging2
where row_num > 1;

select * from layoffs_staging2;


-- 2 : standardize data

select company , trim(company) from layoffs_staging2 ;

update layoffs_staging2
set company = trim(company); 

select distinct industry from layoffs_staging2 order by 1;

update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%';

select  * from layoffs_staging2 ;

select distinct country , trim(country) from layoffs_staging2 order by 1 ;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%' ;

select distinct location from layoffs_staging2 ;



update layoffs_staging2
set  `date`= str_to_date(`date`,'%m/%d/%Y') ;


alter table layoffs_staging2
modify column `date` date ;


-- 3 : null values or blank values

select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null ;


select * from layoffs_staging2 where industry is null or industry = '' ;

select * from layoffs_staging2 where company = 'Airbnb' ;


select T1.industry , T2.industry from layoffs_staging2 T1
join layoffs_staging2 T2
	on  T1.company = T2.company
where T1.industry is NULL and t2.industry is not null ;							

update layoffs_staging2
set industry = null
where industry = '';

update layoffs_staging2 T1
join layoffs_staging2 T2
	on  T1.company = T2.company
set T1.industry = T2.industry 
where T1.industry is NULL and t2.industry is not null ;


-- 4 : remove any columns 

select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null ;



delete from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num ;


-- 5 : exploratory data analytics

select max(total_laid_off) , max(percentage_laid_off)
from layoffs_staging2 ;


select * from layoffs_staging2
where percentage_laid_off=1 
order by total_laid_off desc ;

select * from layoffs_staging2
where percentage_laid_off=1 
order by funds_raised_millions desc ;

select company,sum(total_laid_off) Sum_Laid_off 
from layoffs_staging2
group by company
order by 2 desc ;


select min(`date`) Min_date,max(`date`) max_date from layoffs_staging2 ;

select industry,sum(total_laid_off) Sum_Laid_off 
from layoffs_staging2
group by industry
order by 2 desc ;



select country,sum(total_laid_off) Sum_Laid_off 
from layoffs_staging2
group by country
order by 2 desc ;



select year(`date`),sum(total_laid_off) Sum_Laid_off 
from layoffs_staging2
group by year(`date`)
order by 1 desc ;



select stage,sum(total_laid_off) Sum_Laid_off 
from layoffs_staging2
group by stage
order by 2 desc ;



select substring(`date`,6,2) `Month` , sum(total_laid_off) Sum_Laid_off 
from layoffs_staging2
group by `Month`
order by 1 desc ;

select substring(`date`,1,7) `Month` , sum(total_laid_off) Sum_Laid_off 
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `Month`
order by 1 asc ;



with Rolling_Total as (
	select substring(`date`,1,7) `Month` , sum(total_laid_off) Sum_Laid_off 
	from layoffs_staging2
	where substring(`date`,1,7) is not null
	group by `Month`
	order by 1 asc
)
select `Month` ,Sum_Laid_off,sum(Sum_Laid_off) over(order by `Month`) as rolling_total from Rolling_Total ;




select company , year(`date`),sum(total_laid_off)  total_laid_off
from layoffs_staging2
group by company,year(`date`)
order by 3 asc;

WITH company_year(company, years, total_laid_off) AS (
    SELECT company,YEAR(`date`), SUM(total_laid_off) AS total_laid_off 
    FROM layoffs_staging2
    GROUP BY company, YEAR(`date`)
), 
company_year_rank AS (
    SELECT  *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
    FROM company_year
    WHERE years IS NOT NULL
    ORDER BY ranking
)
SELECT * 
FROM company_year_rank 
WHERE ranking <= 5;
