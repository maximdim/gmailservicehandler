/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.dataimport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import javax.mail.Address;
import javax.mail.FetchProfile;
import javax.mail.Flags;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Part;
import javax.mail.internet.AddressException;
import javax.mail.internet.ContentType;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.mail.search.ComparisonTerm;
import javax.mail.search.ReceivedDateTerm;
import javax.mail.search.SearchTerm;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.tika.Tika;
import org.apache.tika.metadata.HttpHeaders;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.samples.oauth2.OAuth2Authenticator;
import com.sun.mail.imap.IMAPStore;

/**
 * An {@link EntityProcessor} instance which can index emails along with their attachments 
 * Gmail IMAP source using Service Account so you don't need to know passwords for individual
 * user accounts.
 * 
 * This code borrows many parts from existing Solr's {@link MailEntityProcessor}. Too bad
 * MailEntityProcessor is not written with extension in mind - otherwise only authentication
 * mechanism and multi-user support could be added
 * 
 * @author Dmitri Maximovich maxim@maximdim.com
 *
 */
public class GmailServiceUserMailEntityProcessor extends EntityProcessorBase {
  static final Logger LOG = LoggerFactory.getLogger(GmailServiceUserMailEntityProcessor.class);

  // Fields To Index
  // single valued
  private static final String USER_ID = "userId";
  private static final String MESSAGE_ID = "messageId";
  private static final String SUBJECT = "subject";
  private static final String FROM = "from";
  private static final String FROM_CLEAN = "from_clean";
  private static final String SENT_DATE = "sentDate";
  private static final String RECEIVED_DATE = "receivedDate";
  private static final String XMAILER = "xMailer";
  // first 'To' address - need it for sorting. Sort is impossible for multivalued fields
  private static final String TO = "to";
  private static final String TO_CLEAN = "to_clean";
  // hash of the important fields
  private static final String HASH = "hash";
  
  // multi valued
  private static final String TO_CC_BCC = "allTo";
  private static final String TO_CC_BCC_CLEAN = "allTo_clean";
  private static final String FLAGS = "flags";
  private static final String CONTENT = "content";
  private static final String ATTACHMENT = "attachment";
  private static final String ATTACHMENT_NAMES = "attachmentNames";
  // flag values
  private static final String FLAG_ANSWERED = "answered";
  private static final String FLAG_DELETED = "deleted";
  private static final String FLAG_DRAFT = "draft";
  private static final String FLAG_FLAGGED = "flagged";
  private static final String FLAG_RECENT = "recent";
  private static final String FLAG_SEEN = "seen";

  private Tika tika = new Tika();

  private String serviceAccountId;
  private File serviceAccountPkFile;
  private String domain;
  private File timestampFile;
  // <User, Date>
  private Map<String, Date> userTimestamps;

  private List<String> users;
  private List<String> ignoreFrom;
  
  private int userIndex;
  private UserMessagesIterator userMessagesIterator;


  @Override
  protected void firstInit(Context context) {
    //LOG.info("gmail firstInit()");
    super.firstInit(context);
    OAuth2Authenticator.initialize();
  }

  @Override
  public void init(Context context) {
    //LOG.info("gmail init()");
    super.init(context);
    // init counters
    this.userIndex = 0;
    this.userMessagesIterator = null;
    // read config values
    // TODO: check for missing required values and give meaningful error
    this.serviceAccountId = getStringFromContext("serviceAccountId", null);
    this.serviceAccountPkFile = new File(getStringFromContext("serviceAccountPkFile", null));
    this.domain = getStringFromContext("domain", null);
    this.users = Arrays.asList(getStringFromContext("users", null).split(","));
    this.ignoreFrom = Arrays.asList(getStringFromContext("ignoreFrom", null).split(","));
    this.timestampFile = new File(getStringFromContext("timestampFile", null));
    Date oldestDate = getDate(getStringFromContext("oldestDate", "2012/01/01"));
    this.userTimestamps = loadTimestamp(this.timestampFile, oldestDate);
    
    LOG.info("serviceAccountId: "+this.serviceAccountId);
    LOG.info("serviceAccountPkFile: "+this.serviceAccountPkFile);
    LOG.info("domain: "+this.domain);
    LOG.info("users: "+this.users);
    LOG.info("ignoreFrom: "+this.ignoreFrom);
    LOG.info("timestampFile: "+this.timestampFile);
    LOG.info("oldestDate: "+oldestDate);
  }

  @Override
  public void close() {
    saveTimestamp(this.userTimestamps, this.timestampFile);
    super.close();
  }
  
  @Override
  public Map<String, Object> nextRow() {
    //LOG.info("gmail nextRow()");
    while (true) {
      if (this.userIndex >= this.users.size()) { // no more users
        return null;
      }

      String user = this.users.get(this.userIndex);
      String email = user + "@" + this.domain;
      if (this.userMessagesIterator == null) {
        try {
          LOG.info("Starting processing " + email);
          IMAPStore store = getStore(email);
          Date from = this.userTimestamps.get(user);
          this.userMessagesIterator = new UserMessagesIterator(store, from);
          continue;
        } catch (Exception e) {
          throw new DataImportHandlerException(DataImportHandlerException.SEVERE, "Message retreival failed for "
              + email, e);
        }
      }

      if (!this.userMessagesIterator.hasNext()) { // no more messages for this user
        this.userIndex++;
        this.userMessagesIterator = null;
        continue;
      }

      Message message = this.userMessagesIterator.next();
      Map<String, Object> row = getDocumentFromMail(message);
      if (row == null) {
        continue;
      }
      
      // save user
      row.put(USER_ID, user);
      updateMessageId(row);
      
      // update timestamp
      if (row.containsKey(RECEIVED_DATE)) {
        this.userTimestamps.put(user, (Date)row.get(RECEIVED_DATE));
      }
      return row;
    }
  }

  // modify messageId to include user and time (not sure if messageId is unique across many users)
  private void updateMessageId(Map<String, Object> row) {
    String user = (String)row.get(USER_ID);
    String id = (String)row.get(MESSAGE_ID);
    Date receivedDate = (Date)row.get(RECEIVED_DATE);
    SimpleDateFormat df = new SimpleDateFormat(ID_DATE_TIME_FORMAT);
    String date = receivedDate != null ? df.format(receivedDate) : "unknown";
    row.put(MESSAGE_ID, user+":"+date+":"+id);
    
  }
  
  private IMAPStore getStore(String email) throws Exception {
    String authToken = OAuth2Authenticator.getToken(this.serviceAccountPkFile, this.serviceAccountId, email);
    LOG.info("authToken OK");

    IMAPStore store = OAuth2Authenticator.connectToImap("imap.gmail.com", 993, email, authToken, false);
    LOG.info("imapStore for [" + email + "] OK");
    return store;
  }

  static class UserMessagesIterator implements Iterator<Message> {
    private final List<Message> messages;
    private int index;

    public UserMessagesIterator(IMAPStore store, Date fetchFrom) throws MessagingException {
      this.messages = getMessages(store, fetchFrom);
    }

    @Override
    public boolean hasNext() {
      LOG.info(this.index+"/"+this.messages.size());
      return this.index < this.messages.size();
    }

    @Override
    public Message next() {
      return this.messages.get(this.index++);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    private List<Message> getMessages(IMAPStore store, Date fetchFrom) throws MessagingException {
      Folder folder = store.getFolder("[Gmail]/All Mail");
      folder.open(Folder.READ_ONLY);
      LOG.info("imap folder open OK");

      int totalMessages = folder.getMessageCount();
      LOG.info("Total messages: " + totalMessages);

      // IMAP search command disregards time, only date is used
      SearchTerm st = new ReceivedDateTerm(ComparisonTerm.GE, fetchFrom);
      Message[] messages = folder.search(st);
      LOG.info("Search returned: " + messages.length);
      // Fetch profile
      FetchProfile fp = new FetchProfile();
      fp.add(FetchProfile.Item.ENVELOPE);
      fp.add("X-mailer");
      folder.fetch(messages, fp);

      List<Message> result = new ArrayList<Message>();
      for(Message m: messages) {
        if (m.getReceivedDate() != null && m.getReceivedDate().after(fetchFrom)) {
          result.add(m);
        }
      }
      LOG.info("Result filtered to: " + result.size());
      return result;
    }

  }

  private Map<String, Object> getDocumentFromMail(Message mail) {
    Map<String, Object> row = new HashMap<String, Object>();
    try {
      if(addPartToDocument(mail, row, true)) {
        return row;
      }
      return null;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  public boolean addPartToDocument(Part part, Map<String, Object> row, boolean outerMost) throws Exception {
    if (outerMost && part instanceof Message) {
      if (!addEnvelopToDocument(part, row)) {
        return false;
      }
      // store hash
      row.put(HASH, DigestUtils.md5Hex((String)row.get(FROM_CLEAN)+""+(String)row.get(SUBJECT)));
    }

    String ct = part.getContentType();
    ContentType ctype = new ContentType(ct);
    if (part.isMimeType("multipart/*")) {
      Multipart mp = (Multipart) part.getContent();
      int count = mp.getCount();
      if (part.isMimeType("multipart/alternative")) {
        count = 1;
      }
      for (int i = 0; i < count; i++) {
        addPartToDocument(mp.getBodyPart(i), row, false);
      }
    } 
    else if (part.isMimeType("message/rfc822")) {
      addPartToDocument((Part) part.getContent(), row, false);
    } 
    else {
      String disp = part.getDisposition();
      @SuppressWarnings("resource") // Tika will close stream
      InputStream is = part.getInputStream();
      String fileName = part.getFileName();
      Metadata md = new Metadata();
      md.set(HttpHeaders.CONTENT_TYPE, ctype.getBaseType().toLowerCase(Locale.ROOT));
      md.set(TikaMetadataKeys.RESOURCE_NAME_KEY, fileName);
      String content = this.tika.parseToString(is, md);
      if (disp != null && disp.equalsIgnoreCase(Part.ATTACHMENT)) {
        if (row.get(ATTACHMENT) == null) {
          row.put(ATTACHMENT, new ArrayList<String>());
        }
        List<String> contents = (List<String>) row.get(ATTACHMENT);
        contents.add(content);
        row.put(ATTACHMENT, contents);
        if (row.get(ATTACHMENT_NAMES) == null) {
          row.put(ATTACHMENT_NAMES, new ArrayList<String>());
        }
        List<String> names = (List<String>) row.get(ATTACHMENT_NAMES);
        names.add(fileName);
        row.put(ATTACHMENT_NAMES, names);
      } 
      else {
        if (row.get(CONTENT) == null) {
          row.put(CONTENT, new ArrayList<String>());
        }
        List<String> contents = (List<String>) row.get(CONTENT);
        contents.add(content);
        row.put(CONTENT, contents);
      }
    }
    return true;
  }

  private boolean addEnvelopToDocument(Part part, Map<String, Object> row) throws MessagingException {
    MimeMessage mail = (MimeMessage) part;
    Address[] adresses;
    if ((adresses = mail.getFrom()) != null && adresses.length > 0) {
      String from = adresses[0].toString();
      // check if we should ignore this sender
      for(String ignore: this.ignoreFrom) {
        if (from.toLowerCase().contains(ignore)) {
          LOG.info("Ignoring email from "+from);
          return false;
        }
      }
      row.put(FROM, from);
      row.put(FROM_CLEAN, cleanAddress(from));
    }
    else {
      return false;
    }

    List<String> to = new ArrayList<String>();
    if ((adresses = mail.getRecipients(Message.RecipientType.TO)) != null) {
      addAddressToList(adresses, to);
    }
    if ((adresses = mail.getRecipients(Message.RecipientType.CC)) != null) {
      addAddressToList(adresses, to);
    }
    if ((adresses = mail.getRecipients(Message.RecipientType.BCC)) != null) {
      addAddressToList(adresses, to);
    }
    if (!to.isEmpty()) {
      row.put(TO_CC_BCC, to);
      List<String> cleanAddresses = cleanAddresses(to);
      row.put(TO_CC_BCC_CLEAN, cleanAddresses);

      // save first TO address into separate field
      row.put(TO, to.get(0));
      row.put(TO_CLEAN, cleanAddresses.get(0));
    }

    row.put(MESSAGE_ID, mail.getMessageID());
    row.put(SUBJECT, mail.getSubject());

    {
      Date d = mail.getSentDate();
      if (d != null) {
        row.put(SENT_DATE, d);
      }
    }
    {
      Date d = mail.getReceivedDate();
      if (d != null) {
        row.put(RECEIVED_DATE, d);
      }
    }

    List<String> flags = new ArrayList<String>();
    for (Flags.Flag flag : mail.getFlags().getSystemFlags()) {
      if (flag == Flags.Flag.ANSWERED) {
        flags.add(FLAG_ANSWERED);
      } else if (flag == Flags.Flag.DELETED) {
        flags.add(FLAG_DELETED);
      } else if (flag == Flags.Flag.DRAFT) {
        flags.add(FLAG_DRAFT);
      } else if (flag == Flags.Flag.FLAGGED) {
        flags.add(FLAG_FLAGGED);
      } else if (flag == Flags.Flag.RECENT) {
        flags.add(FLAG_RECENT);
      } else if (flag == Flags.Flag.SEEN) {
        flags.add(FLAG_SEEN);
      }
    }
    flags.addAll(Arrays.asList(mail.getFlags().getUserFlags()));
    row.put(FLAGS, flags);

    String[] hdrs = mail.getHeader("X-Mailer");
    if (hdrs != null) {
      row.put(XMAILER, hdrs[0]);
    }
    return true;
  }

  private void addAddressToList(Address[] adresses, List<String> to) throws AddressException {
    for (Address address : adresses) {
      to.add(address.toString());
      InternetAddress ia = (InternetAddress) address;
      if (ia.isGroup()) {
        InternetAddress[] group = ia.getGroup(false);
        for (InternetAddress member : group) {
          to.add(member.toString());
        }
      }
    }
  }

  private static final String EMAIL_PATTERN = 
      "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
      + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
  
  private static final Pattern emailPattern = Pattern.compile(EMAIL_PATTERN);
  
  /**
   * @return 'clean' email address or null if it doesn't look like email address
   */
  private static String cleanAddress(String a) {
    if (a == null || a.trim().length() == 0) {
      return null;
    }
    int i = a.indexOf('<');
    int j = a.indexOf('>', i);
    if (i >= 0 && j > i) {
      String mail = a.substring(i+1, j);
      if (emailPattern.matcher(mail).matches()) {
        return mail.toLowerCase();
      }
    }
    else {
      return a.trim().toLowerCase();
    }
    return null;
  }
  
  private static List<String> cleanAddresses(List<String> aa) {
    List<String> result = new ArrayList<String>();
    for(String a: aa) {
      String email = cleanAddress(a);
      if (email != null) {
        result.add(email);
      }
    }
    return result;
  }

  private Date getDate(String d) {
    SimpleDateFormat df = new SimpleDateFormat(DATE_FORMAT);
    try {
      return df.parse(d);
    } catch (ParseException e) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE, "getFetchDate() failed", e);
    }
  }
  
  private String getStringFromContext(String prop, String ifNull) {
    String v = ifNull;
    String val = context.getEntityAttribute(prop);
    if (val != null) {
      val = context.replaceTokens(val);
      v = val;
    }
    return v;
  }
  
  private static final String DATE_FORMAT = "yyyy-MM-dd";
  private static final String DATE_TIME_FORMAT = DATE_FORMAT+"'T'HH:mm:ssZ";
  private static final String ID_DATE_TIME_FORMAT = "yyyyMMddHHmmss";
  
  /**
   * load saved timestamp file (if available)
   * @param oldestDate 
   * @throws IOException 
   */
  private Map<String, Date> loadTimestamp(File f, Date defaultDate) {
    Map<String, Date> result = new HashMap<String, Date>();
    if (f.exists() && f.canRead()) {
      @SuppressWarnings("resource")
      BufferedReader br = null;
      try {
        br = new BufferedReader(new FileReader(f));
        String line = null;
        SimpleDateFormat df = new SimpleDateFormat(DATE_TIME_FORMAT);
        while((line = br.readLine()) != null) {
          String[] ss = line.split("=");
          if (ss.length != 2) {
            LOG.warn("Don't understand line ["+line+"]");
            continue;
          }
          try {
            result.put(ss[0], df.parse(ss[1]));
          } 
          catch (ParseException e) {
            LOG.warn("Unable to parse date ["+ss[1]+"]");
            continue;
          }
        }
      } 
      catch (IOException e) {
        LOG.error("Error loading user timestamps from "+f.getAbsolutePath()+": "+e.getMessage(), e);
      }
      finally {
        close(br);
      }
    }
    // fill with defaults
    for(String user: this.users) {
      if (!result.containsKey(user)) {
        result.put(user, defaultDate);
      }
    }
    // log
    for(Map.Entry<String, Date> me: result.entrySet()) {
      LOG.info(me.getKey()+"="+me.getValue());
    }
    return result;
  }
  
  private void saveTimestamp(Map<String, Date> data, File f) {
    @SuppressWarnings("resource")
    BufferedWriter bw = null;
    try {
      bw = new BufferedWriter(new FileWriter(f));
      SimpleDateFormat df = new SimpleDateFormat(DATE_TIME_FORMAT);
      for(Map.Entry<String, Date> me: data.entrySet()) {
        String line = me.getKey()+"="+df.format(me.getValue());
        bw.write(line + "\n");
        LOG.info(line);
      }
      bw.flush();
    }
    catch (IOException e) {
      LOG.error("Error saving user timestamps to "+f.getAbsolutePath()+": "+e.getMessage(), e);
    }
    finally {
      close(bw);
    }
  }
  
  private void close(Closeable c) {
    if (c != null) {
      try {
        c.close();
      }
      catch (IOException e) {
        LOG.warn("Error closing Closeable: "+e.getMessage(), e);
      }
    }
  }
  
}
