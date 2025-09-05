# 用于批量创建Git标签

# 脚本需要在PowerShell中运行，并且需要安装Git。定位到当前按文件夹，然后运行.\addtags.ps1

# 标签列表存储在当前目录下的 tags.txt 文件中, 每行一个标签。
$filePath = "tags.txt"
# 检查文件是否存在
if (Test-Path $filePath) {
    # 获取文件大小
    $fileLength = (Get-Item $filePath).Length
    if ($fileLength -eq 0) {
        Write-Host "File is empty"
        Exit
    }
    else {
        Write-Host "File exists and has $fileLength bytes"
    }
}
else {
    Write-Host "File does not exist"
    Exit
}

# 检查是否已经初始化了Git仓库
if (-not (Test-Path .git)) {
    Write-Host "This is not a Git repository. "
    Exit
}
# 读取文件中的所有行
$tags = Get-Content -Path $filePath | Where-Object { $_.trim() -ne "" }
# 获取远程所有的标签
$remoteTags = git ls-remote --tags origin | ForEach-Object { ($_ -split "\t")[-1] } | ForEach-Object { $_ -replace "refs/tags/", "" }

# 定义一个函数来生成新的标签
function Get-NewTag {
    param (
        [string]$baseTag
    )
    # 检查远程是否存在符合模式的标签 - 只匹配完全相同的baseTag后跟-X格式的标签
    $pattern = "^${baseTag}-\d+$"
    $matchingTags = $remoteTags | Where-Object { $_ -match $pattern }
    Write-Host "Matching tags: $($matchingTags -join ', ')"
    if ($matchingTags.Count -eq 0) {
        # 如果没有找到匹配的标签，创建第一个版本标签 a.b.c.0
        return "$baseTag-0"
    }
    else {
        # 找到最大版本号并加1 - 修复版本号递增逻辑
        $maxTag = $matchingTags | ForEach-Object { 
            if ($_ -match "-(\d+)$") { [int]$matches[1] } 
        } | Sort-Object -Descending | Select-Object -First 1
        Write-Host "Max tag: $maxTag"
        $newVersion = $maxTag + 1
        return "$baseTag-$($newVersion)"
    }
}
# 暂存当前分支
git stash
# 切换到master分支
git checkout master
git pull --rebase
git fetch --prune --prune-tags
git merge develop --no-ff
git push
# # 获取所有已存在的标签
# $existingTags = git tag --list

# 对于每个需要的标签
foreach ($tag in $tags) {
    # 去掉前后空格
    $tag = $tag.trim()
    $newTag = Get-NewTag -baseTag $tag
    git tag -a $newTag -m "Tag $newTag"
    Write-Host "Created tag $newTag."
}

# 推送所有的新标签到远程仓库
git push --tags

# 恢复develop分支
git checkout develop
git stash pop

Write-Host "All tags have been pushed to the remote repository."