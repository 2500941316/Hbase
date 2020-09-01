package com.shu.hbase.Dao.upload;

import com.shu.hbase.Tools.TableModel;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.util.Arrays;
import java.util.Comparator;

public class UploadToMvc {
    public static TableModel uploadTomvc(MultipartFile file, String uid, Integer chunk, Integer chunks, HttpServletRequest request, String backId) throws Exception {
        //获取项目的根路径
        String realpath = request.getSession().getServletContext().getRealPath("/");
        String fileId = uid + "_" + System.currentTimeMillis();
        //截取上传文件的类型
        String fileType = file.getOriginalFilename().substring(file.getOriginalFilename().lastIndexOf("."));
        File video = new File(realpath + "final" + uid + "/" + file.getOriginalFilename());
        //先判断文件的父目录是否存在，不存在需要创建；否则报错
        if (!video.getParentFile().exists()) {
            video.getParentFile().mkdirs();
            video.createNewFile();//创建文件
        }
        try {
            if (chunk == null && chunks == null) {//没有分片 直接保存

                file.transferTo(video);
                return MvcToHadoop.createFile(realpath + "final" + uid + "/" + file.getOriginalFilename(), backId, fileId, uid);
            } else {
                //根据guid 创建一个临时的文件夹
                File file2 = new File(realpath + "/" + uid + "/" + file.getOriginalFilename() + "/" + chunk + fileType);
                if (!file2.exists()) {
                    file2.mkdirs();
                }

                //保存每一个分片
                file.transferTo(file2);

                //如果当前是最后一个分片，则合并所有文件
                if (chunk == (chunks - 1)) {
                    File tempFiles = new File(realpath + "/" + uid + "/" + file.getOriginalFilename());
                    File[] files = tempFiles.listFiles();
                    while (true) {
                        if (files.length == chunks) {
                            break;
                        }
                        Thread.sleep(300);
                        files = tempFiles.listFiles();
                    }
                    FileOutputStream fileOutputStream = null;
                    BufferedOutputStream bufferedOutputStream = null;
                    BufferedInputStream inputStream = null;
                    try {
                        //创建流
                        fileOutputStream = new FileOutputStream(video, true);
                        //创建文件输入缓冲流
                        bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
                        byte[] buffer = new byte[4096];//一次读取1024个字节
                        //对这个文件数组进行排序
                        Arrays.sort(files, new Comparator<File>() {
                                    @Override
                                    public int compare(File o1, File o2) {
                                        int o1Index = Integer.parseInt(o1.getName().split("\\.")[0]);
                                        int o2Index = Integer.parseInt(o2.getName().split("\\.")[0]);
                                        if (o1Index > o2Index) {
                                            return 1;
                                        } else if (o1Index == o2Index) {
                                            return 0;
                                        } else {
                                            return -1;
                                        }
                                    }
                                }
                        );
                        for (int i = 0; i < files.length; i++) {
                            File fileTemp = files[i];
                            inputStream = new BufferedInputStream(new FileInputStream(fileTemp));
                            int readcount;
                            while ((readcount = inputStream.read(buffer)) > 0) {
                                bufferedOutputStream.write(buffer, 0, readcount);
                                bufferedOutputStream.flush();
                            }
                            inputStream.close();
                        }
                        bufferedOutputStream.close();
                        return MvcToHadoop.createFile(realpath + "final" + uid + "/" + file.getOriginalFilename(), backId, fileId, uid);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return TableModel.error("上传失败");
                    } finally {
                        if (inputStream != null) {
                            try {
                                for (int i = 0; i < files.length; i++) {
                                    files[i].delete();
                                }
                                tempFiles.delete();
                                inputStream.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        if (fileOutputStream != null) {
                            try {
                                fileOutputStream.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        if (bufferedOutputStream != null) {
                            try {
                                bufferedOutputStream.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return TableModel.error("网络异常");
    }

    public static TableModel removeFile(String fileName, String uid, HttpServletRequest request) {
        String pearsPath = request.getSession().getServletContext().getRealPath("/") + uid + "/" + fileName;//文件夹路径
        String finalPath = request.getSession().getServletContext().getRealPath("/") + "final" + uid + "/" + fileName;//合并文件路径
        String[] cmd1 = new String[]{"/bin/sh", "-c", "rm -rf " + pearsPath};
        String[] cmd2 = new String[]{"/bin/sh", "-c", "rm -rf " + finalPath};
        try {
            Runtime.getRuntime().exec(cmd1);
            Runtime.getRuntime().exec(cmd2);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            new File(pearsPath).deleteOnExit();
            new File(finalPath).deleteOnExit();
            try {
                Thread.sleep(1500);
                if (new File(pearsPath).exists()) {
                    new File(pearsPath).delete();
                }
                if (new File(finalPath).exists()) {
                    new File(finalPath).delete();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return TableModel.success("删除成功");
    }
}



