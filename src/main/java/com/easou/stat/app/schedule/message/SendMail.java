package com.easou.stat.app.schedule.message;

import java.util.List;

public class SendMail {

	public static MailSenderInfo getMailInfo(String mailuser, String Subject, String Content) {
		MailSenderInfo mailInfo = new MailSenderInfo();
		mailInfo.setMailServerHost("smtp.easou.com");
		mailInfo.setMailServerPort("25");
		mailInfo.setValidate(true);
		mailInfo.setFromAddress("cooperate@staff.easou.com");
		mailInfo.setUserName("cooperate@staff.easou.com");
		mailInfo.setPassword("fuckyou");

		mailInfo.setToAddress(mailuser);

		mailInfo.setSubject(Subject);
		mailInfo.setContent(Content);

		return mailInfo;
	}

	@SuppressWarnings("static-access")
	public static void send(String mailuser, String Subject, String Content) {
		MailSender sms = new MailSender();
		// sms.sendTextMail(mailInfo);// 发送文体格式
		sms.sendHtmlMail(SendMail.getMailInfo(mailuser, Subject, Content));// 发送html格式　
	}

	@SuppressWarnings("static-access")
	public static void sendList(List<String> mailuserList, String Subject, String Content) {
		MailSender sms = new MailSender();
		for (int i = 0; i < mailuserList.size(); i++) {
			sms.sendHtmlMail(SendMail.getMailInfo(mailuserList.get(i), Subject, Content));// 发送html格式　
		}
	}

	public static void main(String[] args) {
		SendMail.send("sliven_zhang@staff.easou.com", "Hadoop Job 报警邮件", "ok");
	}
}