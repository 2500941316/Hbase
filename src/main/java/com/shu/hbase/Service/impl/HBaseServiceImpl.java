package com.shu.hbase.Service.impl;

import com.shu.hbase.Dao.HBaseDao;
import com.shu.hbase.Dao.upload.UploadToMvc;
import com.shu.hbase.Pojo.NewGroupInfoVO;
import com.shu.hbase.Pojo.ShareToFileVO;
import com.shu.hbase.Service.interfaces.HBaseService;
import com.shu.hbase.Tools.TableModel;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Service
public class HBaseServiceImpl implements HBaseService {
    @Override
    public TableModel getShares(String userId) throws Exception {
        return HBaseDao.getAllFiles(userId);
    }

    @Override
    public TableModel shareTo(ShareToFileVO shareToFileVO) {
        return HBaseDao.shareTo(shareToFileVO);
    }

    @Override
    public TableModel buildGroup(NewGroupInfoVO newGroupInfoVO) {
        return HBaseDao.buildGroup(newGroupInfoVO);
    }

    @Override
    public TableModel getGroupFile(String gId) {
        return HBaseDao.getGroupFile(gId);
    }

    @Override
    public TableModel getMyShare(String gId, String userId) {
        return HBaseDao.getMyShare(gId, userId);
    }

    @Override
    public TableModel deleteShare(String detSrc, String gId, String uId) {
        try {
            HBaseDao.deleteFnHbase(detSrc, gId, uId);
        } catch (Exception e) {
            return TableModel.error("删除失败");
        }
        return TableModel.success("删除成功");
    }

    @Override
    public TableModel selectFile(String detSrc, String type, String uId, String gId) {
        return HBaseDao.selectFile(detSrc, type, uId, gId);
    }

    @Override
    public TableModel deleteFile(String fileId, String uId) throws IOException {
        return HBaseDao.deleteFile(fileId, uId);
    }

    @Override
    public TableModel checkUserPath(String backId, String department, String uId) {
        return HBaseDao.checkTables(backId, uId, department);
    }

    @Override
    public TableModel getPicFiles(String type, String backId, String uId) {
        return HBaseDao.getFilesByType(type, backId, uId);
    }

    @Override
    public TableModel searchFile(String value, String uId) {
        return HBaseDao.searchFile(value, uId);
    }

    @Override
    public void downLoad(String fileId, String gId, HttpServletResponse response, HttpServletRequest request, String uId) throws IOException {
        HBaseDao.downLoad(fileId, gId, response, request, uId);
    }

    @Override
    public TableModel reNameing(String gid, String reName, String name) {

        return HBaseDao.reNameGroup(gid, reName, name);
    }

    @Override
    public TableModel deleteGroup(String gid, String name) {
        return HBaseDao.deleteGroup(gid, name);
    }

    @Override
    public TableModel getMembersBygid(String gid, String name) {
        return HBaseDao.getMembersBygid(gid, name);
    }

    @Override
    public TableModel uploadTomvc(MultipartFile file, String uid, Integer chunk, Integer chunks, HttpServletRequest request,String backId) throws Exception {
        return  UploadToMvc.uploadTomvc(file, uid, chunk, chunks, request,backId);
    }

    @Override
    public TableModel removeFile(String fileName, String uid,HttpServletRequest request)  {
        return  UploadToMvc.removeFile(fileName, uid, request);
    }


    @Override
    public TableModel buildDirect(String backId,String dirName,String userId) throws IOException {
        return HBaseDao.mkdir(backId,dirName,userId);
    }

}
