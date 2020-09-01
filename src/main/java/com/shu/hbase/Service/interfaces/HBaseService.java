package com.shu.hbase.Service.interfaces;

import com.shu.hbase.Pojo.AnswerInfo;
import com.shu.hbase.Pojo.NewGroupInfoVO;
import com.shu.hbase.Pojo.Question;
import com.shu.hbase.Pojo.ShareToFileVO;
import com.shu.hbase.Tools.TableModel;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;


public interface HBaseService {
    //获取分组信息
    TableModel getShares(String userId) throws Exception;

    TableModel shareTo(ShareToFileVO shareToFileVO) throws IOException;

    TableModel buildGroup(NewGroupInfoVO newGroupInfoVO);

    TableModel getGroupFile(String gId);

    TableModel getMyShare(String gId, String userId);

    TableModel deleteShare(String detSrc,String gId, String uId);

    TableModel selectFile(String detSrc,String type,String uId,String gId);

    TableModel deleteFile(String fileId, String uId) throws IOException;

    TableModel checkUserPath(String backId, String department, String uId);

    TableModel getPicFiles(String type,String backId, String name);

    TableModel searchFile(String value, String name);

    void   downLoad(String fileId, String gId, HttpServletResponse response, HttpServletRequest request,String uId) throws IOException;

    TableModel reNameing(String gid, String reName, String name);

    TableModel deleteGroup(String gid, String name);

    TableModel getMembersBygid(String gid, String name);

    TableModel uploadTomvc(MultipartFile file, String name, Integer chunk, Integer chunks, HttpServletRequest request,String backId) throws Exception;

    TableModel removeFile(String fileName, String name,HttpServletRequest request) throws IOException;

    TableModel buildDirect(String backId, String dirName, String name) throws IOException;
}
