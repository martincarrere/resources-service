package org.epos.api.beans.software;

import org.epos.eposdatamodel.SoftwareApplication;

import java.util.List;

public class SoftwareApplicationResponse {

	private String name;
	private String description;
	private String downloadURL;
	private String installURL;
	private String licenseURL;
	private String mainEntityOfPage;
	private String requirements;
	private String softwareVersion;
	private String softwareStatus;
	private String spatial;
	private String temporal;
	private String fileSize;
	private String timeRequired;
	private String processorRequirements;
	private String memoryrequirements;
	private String storageRequirements;
	private List<String> citation;
	private List<String> operatingSystem;
	private String keywords;

	public SoftwareApplicationResponse(SoftwareApplication softwareApplication) {
		this.name = softwareApplication.getName();
		this.description = softwareApplication.getDescription();
		this.downloadURL = softwareApplication.getDownloadURL();
		this.installURL = softwareApplication.getInstallURL();
		this.licenseURL = softwareApplication.getLicenseURL();
		this.mainEntityOfPage = softwareApplication.getMainEntityOfPage();
		this.requirements = softwareApplication.getRequirements();
		this.softwareVersion = softwareApplication.getSoftwareVersion();
		this.softwareStatus = softwareApplication.getSoftwareStatus();
		this.spatial = softwareApplication.getSpatial();
		this.temporal = softwareApplication.getTemporal();
		this.fileSize = softwareApplication.getFileSize();
		this.timeRequired = softwareApplication.getTimeRequired();
		this.processorRequirements = softwareApplication.getProcessorRequirements();
		this.memoryrequirements = softwareApplication.getMemoryrequirements();
		this.storageRequirements = softwareApplication.getStorageRequirements();
		this.citation = softwareApplication.getCitation();
		this.operatingSystem = softwareApplication.getOperatingSystem();
		this.keywords = softwareApplication.getKeywords();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getDownloadURL() {
		return downloadURL;
	}

	public void setDownloadURL(String downloadURL) {
		this.downloadURL = downloadURL;
	}

	public String getInstallURL() {
		return installURL;
	}

	public void setInstallURL(String installURL) {
		this.installURL = installURL;
	}

	public String getLicenseURL() {
		return licenseURL;
	}

	public void setLicenseURL(String licenseURL) {
		this.licenseURL = licenseURL;
	}

	public String getMainEntityOfPage() {
		return mainEntityOfPage;
	}

	public void setMainEntityOfPage(String mainEntityOfPage) {
		this.mainEntityOfPage = mainEntityOfPage;
	}

	public String getRequirements() {
		return requirements;
	}

	public void setRequirements(String requirements) {
		this.requirements = requirements;
	}

	public String getSoftwareVersion() {
		return softwareVersion;
	}

	public void setSoftwareVersion(String softwareVersion) {
		this.softwareVersion = softwareVersion;
	}

	public String getSoftwareStatus() {
		return softwareStatus;
	}

	public void setSoftwareStatus(String softwareStatus) {
		this.softwareStatus = softwareStatus;
	}

	public String getSpatial() {
		return spatial;
	}

	public void setSpatial(String spatial) {
		this.spatial = spatial;
	}

	public String getTemporal() {
		return temporal;
	}

	public void setTemporal(String temporal) {
		this.temporal = temporal;
	}

	public String getFileSize() {
		return fileSize;
	}

	public void setFileSize(String fileSize) {
		this.fileSize = fileSize;
	}

	public String getTimeRequired() {
		return timeRequired;
	}

	public void setTimeRequired(String timeRequired) {
		this.timeRequired = timeRequired;
	}

	public String getProcessorRequirements() {
		return processorRequirements;
	}

	public void setProcessorRequirements(String processorRequirements) {
		this.processorRequirements = processorRequirements;
	}

	public String getMemoryrequirements() {
		return memoryrequirements;
	}

	public void setMemoryrequirements(String memoryrequirements) {
		this.memoryrequirements = memoryrequirements;
	}

	public String getStorageRequirements() {
		return storageRequirements;
	}

	public void setStorageRequirements(String storageRequirements) {
		this.storageRequirements = storageRequirements;
	}

	public List<String> getCitation() {
		return citation;
	}

	public void setCitation(List<String> citation) {
		this.citation = citation;
	}

	public List<String> getOperatingSystem() {
		return operatingSystem;
	}

	public void setOperatingSystem(List<String> operatingSystem) {
		this.operatingSystem = operatingSystem;
	}

	public String getKeywords() {
		return keywords;
	}

	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}
}
