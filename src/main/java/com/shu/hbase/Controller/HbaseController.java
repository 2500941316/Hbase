package com.shu.hbase.Controller;

import com.shu.hbase.Pojo.*;
import com.shu.hbase.Service.interfaces.HBaseService;
import com.shu.hbase.Tools.TableModel;
import org.apache.hadoop.hbase.TableName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.Size;
import java.io.IOException;
import java.security.Principal;

@RestController
@CrossOrigin
public class HbaseController {

    @Autowired
    HBaseService hBaseService;

    /**
     * 查询某个目录下的文件信息
     *
     * @param
     * @throws IOException
     */
    @GetMapping("selectFile")
    public TableModel selectFile(@Validated @Size(min = 8) @RequestParam("detSrc") String detSrc, @RequestParam("type") String type,
                                 @RequestParam("gId") String gId, Authentication authentication) {

        if (detSrc.isEmpty()) {
            return TableModel.error("参数为空");
        }
        return hBaseService.selectFile(detSrc, type, authentication.getName(), gId);
    }


    /**
     * 获得分享文件组
     *
     * @param
     * @throws IOException
     */
    @GetMapping("getShares")
    public TableModel getShares(Authentication authentication) throws Exception {

        if (authentication.getName().isEmpty()) {
            return TableModel.error("参数为空");
        }
        return hBaseService.getShares(authentication.getName());
    }

    /**
     * 获得某个分组的文件
     *
     * @param
     * @throws IOException
     */
    @GetMapping("getGroupFile")
    public TableModel getGroupFile(@RequestParam("gId") String gId) {

        if (gId.isEmpty()) {
            return TableModel.error("参数为空");
        }
        return hBaseService.getGroupFile(gId);
    }


    /**
     * 共享文件方法
     *
     * @param
     * @throws IOException
     */
    @PostMapping("shareTo")
    public TableModel shareTo(@RequestBody ShareToFileVO shareToFileVO, Authentication authentication) {

        try {
            shareToFileVO.setUId(authentication.getName());
            return hBaseService.shareTo(shareToFileVO);

        } catch (Exception e) {
            return TableModel.error("请先登录");
        }
    }


    /**
     * 新建分组
     *
     * @param
     * @throws IOException
     */
    @PostMapping("buildGroup")
    public TableModel buildGroup(@RequestBody NewGroupInfoVO newGroupInfoVO, Authentication authentication) {
        try {
            newGroupInfoVO.setUId(authentication.getName());
            return hBaseService.buildGroup(newGroupInfoVO);
        } catch (Exception e) {
            return TableModel.error("请先登录");
        }
    }


    /**
     * 获得分组中我的共享文件
     *
     * @param
     * @throws IOException
     */
    @GetMapping("getMyShare")
    public TableModel getMyShare(@RequestParam("gId") String gId, Authentication authentication) {

        if (gId.isEmpty()) {
            return TableModel.error("参数为空");
        }
        String userId = authentication.getName();
        return hBaseService.getMyShare(gId, userId);
    }


    /**
     * 删除共享组中的文件
     *
     * @param
     * @throws IOException
     */
    @GetMapping("deleteShare")
    public TableModel deleteShare(@RequestParam("fileId") String fileId, @RequestParam("gId") String gId, Authentication authentication) {

        if (fileId.isEmpty() || gId.isEmpty()) {
            return TableModel.error("参数为空");
        }
        String uId = authentication.getName();
        return hBaseService.deleteShare(fileId, gId, uId);

    }

    /**
     * 删除文件
     *
     * @param
     * @throws IOException
     */
    @GetMapping("deleteFile")
    public TableModel deleteFile(@RequestParam("fileId") String fileId, HttpServletRequest request, Authentication authentication) throws IOException {

        if (fileId.isEmpty()) {
            return TableModel.error("参数为空");
        }
        String uId = authentication.getName();
        request.getSession().removeAttribute(fileId);
        return hBaseService.deleteFile(fileId, uId);
    }

    /**
     * 检测有无该用户的文件路径存在，如果没有则创建路径
     *
     * @param
     * @throws IOException
     */
    @GetMapping("checkUserPath")
    public TableModel checkUserPath(@Validated @Size(min = 8) @RequestParam("backId") String backId, String department, Authentication authentication) {

        if (backId.isEmpty()) {
            return TableModel.error("参数为空");
        }
        return hBaseService.checkUserPath(backId, department, authentication.getName());
    }


    /**
     * 重命名组名称
     *
     * @param
     * @throws IOException
     */
    @GetMapping("reNameing")
    public TableModel reNameing(@RequestParam("gid") String gid, @RequestParam("reName") String reName, Authentication authentication) {

        if (gid.isEmpty() || reName.isEmpty()) {
            return TableModel.error("参数为空");
        }
        return hBaseService.reNameing(gid, reName, authentication.getName());
    }


    /**
     * 获得分组成员
     *
     * @param
     * @throws IOException
     */
    @GetMapping("getMembersBygid")
    public TableModel getMembersBygid(@RequestParam("gid") String gid, Authentication authentication) {

        if (gid.isEmpty()) {
            return TableModel.error("参数为空");
        }
        return hBaseService.getMembersBygid(gid, authentication.getName());
    }


    /**
     * 删除一个分组
     *
     * @param
     * @throws IOException
     */
    @GetMapping("deleteGroup")
    public TableModel deleteGroup(@RequestParam("gid") String gid, Authentication authentication) {

        if (gid.isEmpty()) {
            return TableModel.error("参数为空");
        }
        return hBaseService.deleteGroup(gid, authentication.getName());
    }

    /**
     * 下载文件
     *
     * @param
     * @throws IOException
     */
    @PostMapping("downLoad")
    public void downLoad(@RequestParam String fileId, String gId, HttpServletResponse response, HttpServletRequest request, Authentication authentication) throws IOException {

        hBaseService.downLoad(fileId, gId, response, request, authentication.getName());
    }


    /**
     * 查找对应类型的文件
     *
     * @param
     * @throws IOException
     */
    @GetMapping("getPicFiles")
    public TableModel getPicFiles(@RequestParam String type, @RequestParam String backId, Authentication authentication) {
        if (type.isEmpty()) {
            return TableModel.error("参数为空");
        }
        return hBaseService.getPicFiles(type, backId, authentication.getName());
    }


    /**
     * 查找文件
     *
     * @param
     * @throws IOException
     */
    @GetMapping("searchFile")
    public TableModel searchFile(@RequestParam String value, @RequestParam String type, Authentication authentication) {

        if (value.isEmpty()) {
            return TableModel.error("参数为空");
        }
        if (type.equals("private"))
            return hBaseService.searchFile(value, authentication.getName());
        else
            return hBaseService.searchFile(value, "00000000");
    }


    /**上传文件到mvc后端
     * @param file
     * @param chunk
     * @param chunks
     * @param request
     */
    @PostMapping("uploadToBacken")
    public TableModel uploadTomvc( @RequestParam MultipartFile file,  Integer chunk,  Integer chunks,String backId,String uid, HttpServletRequest request) throws Exception {
        if (file.isEmpty()) {
            return TableModel.error("参数为空");
        }
            return hBaseService.uploadTomvc(file, uid, chunk, chunks, request,backId);
    }

    /**前端删除正在上传的文件，后端要删除传了一半的文件
     */
    @GetMapping("removeFile")
    public TableModel removeFile(@RequestParam String fileName,Authentication authentication,HttpServletRequest request) throws Exception {
        if (fileName.isEmpty()) {
            return TableModel.error("参数为空");
        }
        return hBaseService.removeFile(fileName, authentication.getName(),request);
    }


    /**
     * 创建文件夹
     *
     * @param
     * @throws IOException
     */
    @GetMapping("buildDirect")
    public TableModel buildDirect(@RequestParam("backId") String backId, @RequestParam("dirName") String dirName, Authentication authentication) throws IOException {

        if (backId.isEmpty()) {
            return TableModel.error("参数为空");
        }
        return hBaseService.buildDirect(backId, dirName, authentication.getName());
    }


}
