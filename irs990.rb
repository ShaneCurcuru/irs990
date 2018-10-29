#!/usr/bin/env ruby
# 
module Irs990
  DESCRIPTION = <<-HEREDOC
  irs990: Fetch/parse IRS 990 XML forms by EIN and return spreadsheets of selected fields
      Caches lists of objectids and xml copies of forms locally
      Pass FIELDS, a list of XPaths into XML 990 data to extract as columns
      Uses Nokogiri.at_xpath(pathname) to map to CSV column names
      Prints status or general errors to stdout
    SETUP: Download index_20??.csv files from IRS AWS endpoint first
    SEE ALSO: https://aws.amazon.com/public-datasets/irs-990/
    TODO:
    - Verify all | alternate fieldnames are equivalent (changes betw. before2012-2013later)
  HEREDOC
  extend self
  require 'yaml'
  require 'csv'
  require 'nokogiri'
  require 'net/http'
  require 'optparse'
  
  # Constants related to the AWS hosted copy of IRS 990 forms
  IRS_AWS_URL = 'https://s3.amazonaws.com/irs-form-990/'
  IRS_EXTENSION = '_public.xml'
  INDEX_GLOB = 'index_201*.csv' # Currently available 1..8
  OBJECT_ID = 'OBJECT_ID'
  EIN = 'EIN'
  TAXPAYER_NAME = 'TAXPAYER_NAME'
  DEFAULT_OUTPUT_CSV = 'irs990output.csv'

  # Listing of major US 501C() FOSS Foundations to analyze
  FOSS_FOUNDATIONS = { # EIN => Name
    '470825376' => 'Apache',
    '460503801' => 'Linux',
    '412203632' => 'Conservancy',
    '113390208' => 'SPI',
    '462060554' => 'Apereo',
    '200963503' => 'OWASP',
    '043594598' => 'Python',
    '270596562' => 'Sahana',
    '412165986' => 'SFLC', 
    '200097189' => 'Mozilla'
    #'142007220' => 'Pro Publica' # Just for fun, not software related
  }
  # Fields from 990 returns we always report
  COMMON_FIELDS = {
    '/Return/ReturnHeader/Filer/EIN' => 'EIN',
    '/Return/ReturnHeader/TaxYr | /Return/ReturnHeader/TaxYear' => 'Tax Year',
    '/Return/ReturnHeader/TaxPeriodEndDt | /Return/ReturnHeader/TaxPeriodEndDate' => 'FY End',
    '/Return/@returnVersion' => 'Form Version',
    '/Return/ReturnHeader/Filer/BusinessName/BusinessNameLine1Txt | /Return/ReturnHeader/Filer/BusinessName/BusinessNameLine1 | /Return/ReturnHeader/Filer/Name/BusinessNameLine1' => 'Business Name', # Note field changed around 2013
    '/Return/ReturnData/IRS990/LegalDomicileStateCd | /Return/ReturnData/IRS990/StateLegalDomicile' => 'State',
    '/Return/ReturnData/IRS990/GrossReceiptsAmt | /Return/ReturnData/IRS990/GrossReceipts' => 'Gross Receipts(lineG)',
    '/Return/ReturnData/IRS990/Organization501c3Ind | /Return/ReturnData/IRS990/Organization501c3' => 'Is a 501(c)(3)?',  # Either/or Organization501cInd
    '/Return/ReturnData/IRS990/Organization501cInd/@organization501cTypeTxt | /Return/ReturnData/IRS990/Organization501c/@typeOf501cOrganization' => 'Is a 501(c)(_)?',
  }
  # Other default Fields from 990 returns (override these with -fFIELDS.YML)
  FIELDS = {
    # IRS990ScheduleA only present if Organization501c3Ind
    '/Return/ReturnData/IRS990ScheduleA/PublicSupportCY170Pct | /Return/ReturnData/IRS990ScheduleA/PublicSupportPertcentage170' => 'Public support %, SchA, Pt2',
    '/Return/ReturnData/IRS990ScheduleA/PublicSupportCY509Pct' => 'Public support %, SchA, Pt3',
    # TODO also look at facts-and-circumstances?  '/Return/ReturnData/IRS990ScheduleA/ThirtyThrPctSuprtTestsCY509Ind' => '33.33 % Tests - Current Year...',
    
    # Fiscal datapoints
    '/Return/ReturnData/IRS990/FederatedCampaignsAmt' => 'Federated campaigns',
    '/Return/ReturnData/IRS990/MembershipDuesAmt' => 'Membership Dues',
    '/Return/ReturnData/IRS990/RelatedOrganizationsAmt' => 'Related org. amt',
    '/Return/ReturnData/IRS990/GovernmentGrantsAmt' => 'Government grants (contrib)',
    '/Return/ReturnData/IRS990/AllOtherContributionsAmt' => 'All other contrib not included in above',
    '/Return/ReturnData/IRS990/NoncashContributionsAmt | /Return/ReturnData/IRS990/DeductibleNonCashContributions' => 'Noncash contrib',
    '/Return/ReturnData/IRS990/TotalContributionsAmt | /Return/ReturnData/IRS990/ContributionsGrantsCurrentYear' => 'Total contrib', # TODO Verify these are close enough to compare
    '/Return/ReturnData/IRS990/TotalProgramServiceRevenueAmt | /Return/ReturnData/IRS990/ProgramServiceRevenueCY' => 'Program service revenue (line 2G)', # TODO Verify these are same meaning field
    '/Return/ReturnData/IRS990/TotalRevenueGrp/TotalRevenueColumnAmt | /Return/ReturnData/IRS990/TotalRevenue/TotalRevenueColumn' => 'Total revenue',
    '/Return/ReturnData/IRS990/TotalRevenueGrp/RelatedOrExemptFuncIncomeAmt' => 'Total related revenue',
    '/Return/ReturnData/IRS990/TotalRevenueGrp/UnrelatedBusinessRevenueAmt' => 'Total unrelated revenue',
    '/Return/ReturnData/IRS990/TotalRevenueGrp/ExclusionAmt' => 'Total excluded revenue',
    '/Return/ReturnData/IRS990/TotalFunctionalExpensesGrp/TotalAmt | /Return/ReturnData/IRS990/TotalExpensesCurrentYear' => 'Total Func. Expenses',
    '/Return/ReturnData/IRS990/TotalFunctionalExpensesGrp/ProgramServicesAmt | /Return/ReturnData/IRS990/TotalProgramServiceExpense' => 'Program Func. Expenses',
    '/Return/ReturnData/IRS990/TotalFunctionalExpensesGrp/ManagementAndGeneralAmt | /Return/ReturnData/IRS990/OtherExpensesCurrentYear' => 'Admin Func. Expenses',
    '/Return/ReturnData/IRS990/TotalFunctionalExpensesGrp/FundraisingAmt | /Return/ReturnData/IRS990/TotalFundrsngExpCurrentYear' => 'Fundraising Func. Expenses',
    '/Return/ReturnData/IRS990/CYSalariesCompEmpBnftPaidAmt | /Return/ReturnData/IRS990/SalariesEtcCurrentYear' => 'Salaries etc expenses',
    '/Return/ReturnData/IRS990/TotalAssetsGrp/BOYAmt | /Return/ReturnData/IRS990/TotalAssetsBOY' => 'Total assets, start year',
    '/Return/ReturnData/IRS990/TotalAssetsGrp/EOYAmt | /Return/ReturnData/IRS990/TotalAssetsEOY' => 'Total assets, end year',
    '/Return/ReturnData/IRS990/TotalLiabilitiesGrp/BOYAmt | /Return/ReturnData/IRS990/TotalLiabilitiesBOY' => 'Total liabilities, start year',
    '/Return/ReturnData/IRS990/TotalLiabilitiesGrp/EOYAmt | /Return/ReturnData/IRS990/TotalLiabilitiesEOY' => 'Total liabilities, end year',
    '/Return/ReturnData/IRS990/TotLiabNetAssetsFundBalanceGrp/BOYAmt | /Return/ReturnData/IRS990/NetAssetsOrFundBalancesBOY' => 'Total liabilities and net assets/fund balances, start year',
    '/Return/ReturnData/IRS990/TotLiabNetAssetsFundBalanceGrp/EOYAmt | /Return/ReturnData/IRS990/NetAssetsOrFundBalancesEOY' => 'Total liabilities and net assets/fund balances, end year',

    # Governance datapoints
    '/Return/ReturnData/IRS990/MissionDesc | /Return/ReturnData/IRS990/ActivityOrMissionDescription' => 'Mission',
    '/Return/ReturnData/IRS990/OwnWebsiteInd' => '990 avail on own website', # Not present pre-2013
    '/Return/ReturnData/IRS990/OtherWebsiteInd' => '990 avail on other website', # Not present pre-2013
    '/Return/ReturnData/IRS990/UponRequestInd' => '990 avail upon request', # Not present pre-2013
    '/Return/ReturnData/IRS990/Desc | /Return/ReturnData/IRS990/Description' => 'Program Accomplishments', # TODO Verify these fields are the same; note also this is only first of a list
    '/Return/ReturnData/IRS990/GoverningBodyVotingMembersCnt | /Return/ReturnData/IRS990/NbrVotingMembersGoverningBody' => 'Num of voting governing body members',
    '/Return/ReturnData/IRS990/IndependentVotingMemberCnt | /Return/ReturnData/IRS990/NbrIndependentVotingMembers' => 'Num of independent voting members',
    '/Return/ReturnData/IRS990/TotalEmployeeCnt | /Return/ReturnData/IRS990/TotalNbrEmployees' => 'Num of employees',
    '/Return/ReturnData/IRS990/TotalVolunteersCnt | /Return/ReturnData/IRS990/TotalNbrVolunteers' => 'Num of volunteers',
    '/Return/ReturnData/IRS990/MembersOrStockholdersInd | /Return/ReturnData/IRS990/MembersOrStockholders' => 'Org has members or stockholders?',
    '/Return/ReturnData/IRS990/ElectionOfBoardMembersInd | /Return/ReturnData/IRS990/ElectionOfBoardMembers' => 'Org has persons who had power to elect or appoint board members?',
    '/Return/ReturnData/IRS990/DecisionsSubjectToApprovaInd | /Return/ReturnData/IRS990/DecisionsSubjectToApproval' => 'Governance decisions subject to approval-outside governing body?',
    '/Return/ReturnData/IRS990/MinutesOfGoverningBodyInd | /Return/ReturnData/IRS990/MinutesOfGoverningBody' => 'Org documents minutes of governing body?',
    '/Return/ReturnData/IRS990/MinutesOfCommitteesInd | /Return/ReturnData/IRS990/MinutesOfCommittees' => 'Org documents commtte minutes?',
    '/Return/ReturnData/IRS990/OfficerMailingAddressInd | /Return/ReturnData/IRS990/OfficerMailingAddress' => 'Officers not be reached at mailing address?',
    '/Return/ReturnData/IRS990/Form990ProvidedToGvrnBodyInd | /Return/ReturnData/IRS990/Form990ProvidedToGoverningBody' => 'Form 990 provided to governing body?',
    '/Return/ReturnData/IRS990/ConflictOfInterestPolicyInd | /Return/ReturnData/IRS990/ConflictOfInterestPolicy' => 'Org COI policy?',
    '/Return/ReturnData/IRS990/AnnualDisclosureCoveredPrsnInd | /Return/ReturnData/IRS990/AnnualDisclosureCoveredPersons' => 'Annual disclosure of COIs?',
    '/Return/ReturnData/IRS990/RegularMonitoringEnfrcInd | /Return/ReturnData/IRS990/RegularMonitoringEnforcement' => 'Monitoring of COI policy?',
    '/Return/ReturnData/IRS990/WhistleblowerPolicyInd | /Return/ReturnData/IRS990/WhistleblowerPolicy' => 'Written whistleblower policy?',
    '/Return/ReturnData/IRS990/DocumentRetentionPolicyInd | /Return/ReturnData/IRS990/DocumentRetentionPolicy' => 'Org has written document retention policy?',
    '/Return/ReturnData/IRS990/CompensationProcessCEOInd | /Return/ReturnData/IRS990/CompensationProcessCEO' => 'Compensation process CEO?',
    '/Return/ReturnData/IRS990/CompensationProcessOtherInd | /Return/ReturnData/IRS990/CompensationProcessOther' => 'Compensation process other?',
    '/Return/ReturnData/IRS990/IndependentAuditFinclStmtInd | /Return/ReturnData/IRS990/IndependentAuditFinancialStmt' => 'Independently audited?',
    '/Return/ReturnData/IRS990/ConsolidatedAuditFinclStmtInd | /Return/ReturnData/IRS990/ConsolidatedAuditFinancialStmt' => 'Consolidated audit?'
    # TODO: add listing of [FormAndLineReferenceDesc, ExplanationTxt]
    #   '/Return/ReturnData/IRS990ScheduleO/SupplementalInformationDetail/ExplanationTxt' => 'Form; part and line number reference explanation',
    #   '/Return/ReturnData/IRS990ScheduleO/SupplementalInformationDetail/FormAndLineReferenceDesc' => 'Form; part and line number reference'
  }
  
  # Scan index_*.csv listing spreadsheet for ein
  # @return [OBJECT_ID, TAXPAYER_NAME] if found; nil if error
  def indexcsv2objids(file, ein)
    CSV.foreach(file, headers: true, encoding: 'UTF-8') do |row|
      if row[EIN] == ein
        return [row[OBJECT_ID], row[TAXPAYER_NAME]]
      end
    end
    return nil
  end
  
  # Scan all dir/index_*.csv for ein and cache results in ein.yml
  # @return [[OBJECT_ID, TAXPAYER_NAME], ...] 
  def ein2objectids(dir, ein)
    cache = File.join(dir, "#{ein}.yml")
    if File.file?(cache) # TODO: If new index files are downloaded, doesn't add new objids
      return YAML.load_file(cache)
    end
    eins = []
    Dir[File.join(dir, INDEX_GLOB).untaint].each do |f|
      x = indexcsv2objids(f.untaint, ein)
      eins << x if x
    end
    if not eins.empty?
      File.open(cache, "w") do |f|
        f.write(YAML.dump(eins))
      end
    end
    return eins
  end
  
  # Fetch objid from the AWS endpoint of IRS 990 forms
  # @return http response object
  def fetch_objid(objid)
    uri = URI.parse("#{IRS_AWS_URL}#{objid}#{IRS_EXTENSION}")
    Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == 'https') do |https|
      request = Net::HTTP::Get.new(uri.request_uri)
      response = https.request(request)
      return response
    end
  end
  
  # Get path/filename for objid (a 990 return file in xml)
  # Use local if exists; otherwise retrieve from AWS and cache
  # @return local path/file of the return; nil if error
  def get_return(dir, ein, objid)
    rdir = File.join(dir, ein)
    rfile = File.join(rdir, "#{objid}#{IRS_EXTENSION}")
    if File.file?(rfile)
      return rfile
    end
    puts "Fetching #{ein} - #{objid} from AWS..."
    response = fetch_objid(objid)
    if response.code == '200' then
      Dir.mkdir(rdir) if not File.directory?(rdir)
      File.open(rfile, "w") do |f|
        f.write(response.body)
      end
      return rfile
    else
      puts "ERROR:get_return(#{objid}) returned code #{response.code.inspect}"
      return nil
    end
  end
  
  # Return list of local filenames of all available returns for ein
  # @return [rfile, rfile2, ...]
  def get_returns(dir, ein)
    objids = ein2objectids(dir, ein)
    rfiles = []
    objids.each do |ary|
      tmp = get_return(dir, ein, ary[0])
      rfiles << tmp if tmp
    end
    return rfiles
  end
  
  # Parse a 990 file for ein and gather field data as array
  # @param fields hash of xpaths => desc to nodes to retrieve text values from
  # @return ['val', nil, 'val3', ...] of fields data; nil used when xpath selects null
  # @return ErrorString if error
  def return2ary(file, fields = FIELDS)
    values = []
    fld = ''
    allfields = COMMON_FIELDS.merge(fields) # MAINTENANCE: ensure ein*2csv methods do this too
    begin
      doc = Nokogiri::XML(File.open(file))
      doc.remove_namespaces! # Yes, I do understand what this does
      allfields.keys.each do |f|
        fld = f # Capture for error reporting
        begin
          n = doc.at_xpath(f)
        rescue
          # no-op; just leave nil
        end
        n ? (values << n.text) : (values << nil) # Ensure we fill all columns
      end
      return values
    rescue StandardError => e
      # TODO should we barf on error, or continue loop?
      return "ERROR:return2ary(#{file}): #{fld} #{e.message}"
    end
  end
  
  # Parse dir of 990 files for ein and gather field data as array by return
  # @param fields hash of xpaths => desc to nodes to retrieve text values from
  # @return [['val', nil, 'val3', ...], ...] of fields data from each 990
  def returns2arys(dir, ein, fields)
    rows = []
    rfiles = get_returns(dir, ein)
    rfiles.each do |file|
      tmp = return2ary(file, fields)
      tmp.kind_of?(Array) ? (rows << tmp) : (puts "ERROR:returns2arys(#{file}) #{tmp}")
    end
    return rows
  end
  
  # Write out 990 data from single ein into CSV
  # @param fields hash of xpaths => desc to nodes to retrieve text values from
  # Side Effect: writes outfile of CSV data
  def ein2csv(dir, ein, fields, outfile)
    rows = []
    tmp = returns2arys(dir, ein, fields)
    tmp.each do |r|
      rows << r if r
    end
    if rows.empty?
      raise ArgumentError, "Could not find any data for EIN #{ein}, skipping write of csv"
    end 
    CSV.open(outfile, "w") do |csv|
      csv << COMMON_FIELDS.merge(fields).values # MAINTENANCE: ensure return2ary method does this too
      rows.each do |row|
        csv << row
      end
    end
  end
  
  # Parse 990 files from list of eins into csv
  # @param fields array of xpaths to nodes to retrieve text values from
  # Side Effect: writes outfile of CSV data
  def eins2csv(dir, eins, fields, outfile)
    rows = []
    eins.each do |ein|
      p ein
      tmp = returns2arys(dir, ein, fields)
      tmp.each do |r|
        rows << r if r
      end
    end
    if rows.empty?
      raise ArgumentError, "Could not find any data for all EINs, skipping write of csv"
    end 
    CSV.open(outfile, "w") do |csv|
      csv << COMMON_FIELDS.merge(fields).values # MAINTENANCE: ensure return2ary method does this too
      rows.each do |row|
        csv << row
      end
    end
  end

  # ## ### #### ##### ######
  # Check commandline options
  def parse_commandline
    options = {}
    OptionParser.new do |opts|
      opts.on('-h') { puts "#{DESCRIPTION}\n#{opts}"; exit }
      opts.on('-dDIRECTORY', '--directory DIRECTORY', 'Local directory to store downloaded XML returns in') do |dir|
        if File.directory?(dir)
          options[:dir] = dir
        else
          raise ArgumentError, "-d #{dir} is not a valid directory"
        end
      end
      opts.on('-oOUTFILE.CSV', '--out OUTFILE.CSV', 'Output filename to write spreadsheet of data to') do |out|
        options[:out] = out
      end
      opts.on('-fFIELDS.YML', '--fields FIELDS.YML', 'YAML file of hash of //ReturnData/xpath/IRSfield => Field Description (optional)') do |fyaml|
        options[:fyaml] = fyaml
      end
      opts.on('-eEINNUMBER', '--ein EINNUMBER', 'EIN of a single organization to download and parse 990\'s from (optional)') do |ein|
        options[:ein] = ein
      end
      opts.on('-af', 'Download and parse all major FOSS Foundations data (may take a while on first run)') do |af|
        options[:af] = true
      end
      opts.on('-h', '--help', 'Print help for this program') do
        puts opts
        exit
      end
      begin
        opts.parse!
      rescue OptionParser::ParseError => e
        $stderr.puts e
        $stderr.puts opts
        exit 1
      end
    end
    return options
  end
  
  # ### #### ##### ######
  # Main method for command line use
  if __FILE__ == $PROGRAM_NAME
    options = parse_commandline
    options[:dir] ||= Dir.pwd
    options[:out] ||= DEFAULT_OUTPUT_CSV
    fields = FIELDS
    if options[:yaml]
      fields = YAML.load_file(options[:yaml])
    end
    
    if options[:ein]
      puts "Finding data for EIN #{options[:ein]} and parsing..."
      ein2csv(options[:dir], options[:ein], fields, options[:out])
      
    elsif options[:af]
      puts "Finding data for all common FOSS Foundations... may take a while on first run"
      eins2csv(options[:dir], FOSS_FOUNDATIONS.keys, fields, options[:out])
      
    else
      raise ArgumentError, "Required: either -eEIN or -af (try -h for help)"
    end
    puts "DONE: check #{options[:out]} and ?????????.yml files for data"
  end
end
