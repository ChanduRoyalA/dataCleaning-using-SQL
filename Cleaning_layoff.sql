select * from layoffs;

select * from layoffs_staging;

select * from (select *,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as rownum from layoffs_staging) x where x.rownum >1 ;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select * from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions)
 as rownum 
 from 
 layoffs_staging;
 

delete from layoffs_staging2 where row_num=2;

select * from layoffs_staging2 where row_num =2;

SET SQL_SAFE_UPDATES = 0;

-- 2. standardizing data

select * from layoffs_staging2;

select distinct(company), TRIM(company) from layoffs_staging2;

update layoffs_staging2 
set company = trim(company);


update layoffs_staging2
set industry = "Crypto" where industry LIKE "Crypto%";

select * from layoffs_staging2 where industry LIKE "Crypto%";

select country from layoffs_staging2 where country like "United States%";

update layoffs_staging2 
set country = "United States"  where country like "United States%";

select date,str_to_date(`date`,"%m/%d/%Y") from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,"%m/%d/%Y") ;

alter table layoffs_staging2
modify column `date` date;


-- 3 null values and unknown values;
select * from layoffs_staging2 where total_laid_off is NULL and percentage_laid_off is NULL;

select * from layoffs_staging2 where industry is NULL or industry = "";

update layoffs_staging2 
set industry = null
where industry = '';

select * 
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company and t1.location = t2.location
where ( t1.industry is NULL or t1.industry='') and t2.industry is NOT null;


update layoffs_staging2 t1 
join layoffs_staging2 t2
on t1.company = t2.company and t1.location = t2.location
set t1.industry = t2.industry 
where ( t1.industry is NULL or t1.industry='') and t2.industry is NOT null;


select * from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;


delete from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null; 


alter table layoffs_staging2
drop column row_num;

select * from layoffs_staging2;
