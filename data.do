/*################################################
* Drawback: Identify beneficiary nandina codes
################################################*/
* Choose computer
if "`c(username)'" == "Jorge" {
    global root "d:/dropbox"
   	global main "${root}/projects/cies_drawback"
   	global tradedata "${data}/peru/tradedata"
}
else if "`c(username)'" == "Max" {
	global root "T:/Dropbox/Research/Proyectos/PERU"
	global main "${root}/cies_drawback"
	global tradedata "T:/Dropbox/Research/Datos/Trade/Trade_Peru/tradedata"
	global temp "T:\Temporal" 
}
else if "`c(username)'" == "Antonio" {
	global root "t:/dropbox"
	global main "${root}/Research/Proyectos/YALE/Import_Inertia"
}
clear all
set more off, permanently

*####################################################
* Prepare initial data files
*####################################################
{
	*---------------------------------------------------
	* Drawback Rates
	*---------------------------------------------------
	{
		clear
		set obs 30
		gen year = 1994 + _n
		expand 12

		bysort year : gen month = _n 

		gen dbrate =	0.05 * 	( 								  year 				 <= 2008 	) + ///
						0.08 * 	( 2009 <= year 					& year * 100 + month <  201007	) + ///
						0.065 * ( 201007 <= year * 100 + month  & year 				 <  2011 	) + ///
						0.05 * 	( 2011 <= year 					& year 				 <  2015	) + ///
						0.04 * 	( 2015 <= year 					& year 				 <  2016	) + ///
						0.03 * 	( 2016 <= year 					& year * 100 + month <  201610  ) + ///
						0.04 * 	( 201610 <= year * 100 + month	& year 				 <  2018	) //

		save "${main}/data/dbrate.dta" , replace
	}
	*---------------------------------------------------
	* Exchange Rate
	*---------------------------------------------------
	{
		* Exchange Rate from BCRP
		* https://estadisticas.bcrp.gob.pe/estadisticas/series/mensuales/resultados/PN01207PM/html
		import excel "${main}/data/tipo_de_cambio_nominal_promedio.xlsx", sheet("Mensuales") cellrange(A2:B275) firstrow case(lower) clear

		* Renaming
		rename tipodecambiopromediodelpe exchangerate

		* Year (2-Digits)
		gen year2 = regexr(a,"[a-zA-Z]+","")

		* Month
		gen id = _n
		bysort year (id) : gen month = _n
		tostring month,replace

		* Year (4-Digits)
		gen year = year( date( "01/" + month +"/"+ year2, "DMY"  , 2020))

		* Keeping useful data
		keep year month exchangerate
		destring month,replace
		save "${main}/data/exchangerate.dta" , replace
	}
	*---------------------------------------------------
	* Drawback application outcomes
	*---------------------------------------------------
	{
		* Outcomes
		use "${main}/data/dbdata0_sols.dta", clear
		drop if statusx == 9 // drop if no tiene
		save "${temp}/temp0.dta", replace

		use "${main}/data/dbdata1_sols_details.dta", clear
		merge 1:1 ruc customs numrec daterec using ${temp}/temp0.dta, keepusing(status statusx datesol)
		drop _merge

		* Amend status if missing
		replace statusx = 1 if status == "" & obs != ""
		order datesol, after(numsol)
		
		save ${temp}/tempoutcomes.dta, replace
	}
    *---------------------------------------------------
    * Export DAMS
    *---------------------------------------------------
    {
        * Export duas
        use "${main}/data/dbdata2_exportdams.dta", clear
        * We need to collapse because FOB values are disaggregated within customs+ruc+numdec+nserie
        * see README_DATA.pdf
        collapse (sum) fob, by(customs_sol fecrec daterec numrec yearsol numsol year customs numdec nandina nserie)
        * Notes: there are 19,026 numrecs that did not released the export declarations behind the application
        * Mos of these 19,026 (exactly 18,444) were rejected Drawback applications.
        * Also there are about 3.7 millions of export declarations that are matched to DB applications
        * some of which are linked to 2 or more DB applications. That is, there are applications that are rejected
        * and are resubmitted, possible many times untile accepted.
        merge m:1 customs_sol daterec numrec using ${temp}/tempoutcomes.dta 
        drop _merge
        order datesol, after(numsol)

        * Reduce original categories in statusx to only 0 accepted and 1 rejected
        replace statusx = 1 if rejected == 1 // if REC was rejected
        bys year customs numdec nserie: egen maxreject= max(statusx)
        bys year customs numdec nserie: egen minreject= min(statusx)
        replace statusx = 1 if minreject == 0 & statusx == 99
        replace statusx = 1 if statusx == 99 // only 1 left
        drop minreject maxreject
        * otherdebt = (statusx == 2 | statusx == 3)
        recode statusx 2 =0 3 =0 // Other and debt recoded as accepted

        * Number of recs 
        bys year customs numdec nserie: egen nrecs = nvals(daterec numrec)

        * Time between recs
        * Need to sort using daterec too
        sort year customs numdec nserie daterec
        by year customs numdec nserie: gen daysrec = daterec-daterec[_n-1]

        * Time last rec and sol
        gen dayssol = datesol - daterec if datesol !=.

        * Change in value submitted for drawback
        * Need to sort using daterec too
        sort year customs numdec nserie daterec
        by year customs numdec nserie: gen dfob = fob - fob[_n-1]

        gsort year customs numdec nserie daterec -rejected
        by year customs numdec nserie: gen seq = _n
        by year customs numdec nserie: gen seqtot = _N
        order year customs numdec nserie daterec -rejected seq* fob*

        * Keeping only the last value of fob. This is specially important for 
        * counting repeated values
        gen fobfinal = .
        replace fobfinal = fob if statusx == 0
        replace fobfinal = fob if statusx == 1 & seq == seqtot

        gen fobtotfinal = .
        replace fobtotfinal = fobtot if statusx == 0
        replace fobtotfinal = fobtot if statusx == 1 & seq == seqtot

        format fobfinal fobtotfinal %15.0fc
        drop seq seqtot

        * Labels
        label var fobfinal "Final FOB value, in numdec-series"
        label var fobtotfinal "Final FOB value, in sol"
        label var daysrec "Days between recs"
        label var dayssol "Days from last rec to sol"
        label var nrecs "Number of attempted recs"
        label var nrecs "Sequential numbering at the year-customs-numdec-nserie-rejected"
        label var nrecs "Number of observations at the year-customs-numdec-nserie-rejected"

        save "${temp}/dbdata2_exportdams2.dta", replace
    }
    *---------------------------------------------------
    * Collapsing Export DAMS Data
    *---------------------------------------------------
    {
        * Collapse to have data at the year-customs-numdec-series level
        use "${temp}/dbdata2_exportdams2.dta", clear
        drop if numdec == .
        * Preserving labels before collapse
        foreach v of var * {
            local l`v' : variable label `v'
            if `"`l`v''"' == "" {
            local l`v' "`v'"
            }
        } //"
        collapse (sum) fobfinal daysrec dayssol ///
            (max) nrecs fobtotfinal fianza fianza_date fianza_amount ///
            (min) statusx, ///
            by(year ruc customs numdec nserie nandina)
        * Restoring labels
        foreach v of var * {
            label var `v' "`l`v''"
        }  

        format fob* fianza %15.0fc  
        gen delay = daysrec + dayssol*(dayssol>=0)
        order delay, after(dayssol)
        label var delay "Days the whole application took"
        save "${main}/data/drawback_exportdams.dta", replace
    }
}










